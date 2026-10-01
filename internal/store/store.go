// Package store is Atlas's durable control-plane state.
//
// The store exposes transactions, not operations. Callers compose the reads and
// writes they need inside Update, and either all of them commit or none do. This is
// what makes "create the attempt, move the job to ASSIGNED, and reserve the worker's
// resources" a single atomic fact rather than three hopeful ones.
//
// Two correctness rules are enforced here rather than by convention:
//
//   - Job and attempt states only change through TransitionJob / TransitionAttempt,
//     which validate the edge against internal/state and append to the audit log.
//     Terminal states are therefore absorbing by construction (invariant I1).
//   - SaveWorker rejects negative or oversubscribed allocations, so invariant I2
//     cannot be violated by a buggy caller; the transaction simply fails.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/types"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned by the Get* methods when a row does not exist.
var ErrNotFound = errors.New("not found")

// ErrCapacityExceeded is returned when a write would oversubscribe a worker or drive
// an allocation negative.
var ErrCapacityExceeded = errors.New("worker capacity violated")

// Store owns the SQLite database holding all control-plane state.
type Store struct {
	// write is pinned to a single connection. SQLite permits one writer; making
	// that explicit removes SQLITE_BUSY from the failure model entirely.
	write *sql.DB
	// read is a pool. Under WAL, readers do not block the writer.
	read *sql.DB

	// writeMu serializes write transactions in-process. The single write
	// connection would serialize them anyway, but holding the mutex around the
	// whole transaction keeps "one transaction at a time" true for the audit log
	// sequence too.
	writeMu sync.Mutex

	now func() time.Time
}

// Options configures Open.
type Options struct {
	// Path is the SQLite file. ":memory:" is rejected because the two handles
	// would see different databases; use a temp file instead.
	Path string
	// Now overrides the clock. Tests use this; production leaves it nil.
	Now func() time.Time
}

// Open creates or opens the Atlas database and applies the schema.
func Open(opts Options) (*Store, error) {
	if opts.Path == "" || opts.Path == ":memory:" {
		return nil, fmt.Errorf("store: a file path is required (in-memory databases are not shared between the read and write handles)")
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	// synchronous=FULL is the point of the exercise: a job is acknowledged to a
	// client only once its row is durable, so a power loss cannot lose an
	// accepted job (invariant I6).
	dsn := opts.Path + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"

	w, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open write handle: %w", err)
	}
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	w.SetConnMaxLifetime(0)

	if _, err := w.Exec(schema); err != nil {
		w.Close()
		return nil, fmt.Errorf("store: apply schema: %w", err)
	}

	r, err := sql.Open("sqlite", dsn)
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("store: open read handle: %w", err)
	}
	r.SetMaxOpenConns(8)

	return &Store{write: w, read: r, now: nowFn}, nil
}

// Close releases both handles.
func (s *Store) Close() error {
	err1 := s.read.Close()
	err2 := s.write.Close()
	return errors.Join(err1, err2)
}

// Now returns the store's clock.
func (s *Store) Now() time.Time { return s.now() }

// Tx is a transaction scope. Methods on Tx are the only way to touch persisted state.
type Tx struct {
	tx    *sql.Tx
	now   time.Time
	write bool
}

// Now returns the timestamp fixed at the start of the transaction. Using one instant
// for a whole transaction keeps derived values (enqueued_at, lease expiry, audit
// timestamps) mutually consistent.
func (t *Tx) Now() time.Time { return t.now }

// Update runs fn inside a write transaction, committing if fn returns nil.
func (s *Store) Update(ctx context.Context, fn func(*Tx) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	t := &Tx{tx: tx, now: s.now(), write: true}
	if err := fn(t); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}

// View runs fn inside a read-only transaction.
func (s *Store) View(ctx context.Context, fn func(*Tx) error) error {
	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("store: begin read: %w", err)
	}
	defer tx.Rollback()
	return fn(&Tx{tx: tx, now: s.now()})
}

// ---------------------------------------------------------------- time helpers

func toUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromUnix(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func toUnixPtr(t *time.Time) sql.NullInt64 {
	if t == nil || t.IsZero() {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: t.UnixNano(), Valid: true}
}

func fromUnixPtr(n sql.NullInt64) *time.Time {
	if !n.Valid {
		return nil
	}
	t := time.Unix(0, n.Int64).UTC()
	return &t
}

func toInt32Ptr(n sql.NullInt64) *int32 {
	if !n.Valid {
		return nil
	}
	v := int32(n.Int64)
	return &v
}

func fromInt32Ptr(v *int32) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*v), Valid: true}
}

func marshalJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}

// ---------------------------------------------------------------- audit log

func (t *Tx) appendTransition(kind, jobID, attemptID, from, to, reason string) error {
	_, err := t.tx.Exec(
		`INSERT INTO transitions (kind, job_id, attempt_id, from_state, to_state, reason, at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		kind, jobID, attemptID, from, to, reason, t.now.UnixNano())
	if err != nil {
		return fmt.Errorf("store: append transition: %w", err)
	}
	return nil
}

// Transitions returns the audit log for one job, oldest first. Pass an empty jobID to
// read the whole log.
func (t *Tx) Transitions(jobID string) ([]types.Transition, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if jobID == "" {
		rows, err = t.tx.Query(`SELECT seq, kind, job_id, attempt_id, from_state, to_state, reason, at FROM transitions ORDER BY seq`)
	} else {
		rows, err = t.tx.Query(`SELECT seq, kind, job_id, attempt_id, from_state, to_state, reason, at FROM transitions WHERE job_id = ? ORDER BY seq`, jobID)
	}
	if err != nil {
		return nil, fmt.Errorf("store: query transitions: %w", err)
	}
	defer rows.Close()

	var out []types.Transition
	for rows.Next() {
		var tr types.Transition
		var at int64
		if err := rows.Scan(&tr.Seq, &tr.Kind, &tr.JobID, &tr.AttemptID, &tr.FromState, &tr.ToState, &tr.Reason, &at); err != nil {
			return nil, err
		}
		tr.At = fromUnix(at)
		out = append(out, tr)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------- jobs

const jobColumns = `job_id, idempotency_key, client_id, state, priority, cpu_millis, memory_bytes,
	image, command, env, max_attempts, attempt_count, timeout_ns, retry_on_process_exit,
	retry_on_timeout, created_at, updated_at, enqueued_at, eligible_at, deadline_at,
	current_attempt_id, exit_code, failure_class, message, spec_hash`

func scanJob(sc interface{ Scan(...any) error }) (*types.Job, error) {
	var (
		j          types.Job
		cmdJSON    string
		envJSON    string
		timeoutNs  int64
		createdAt  int64
		updatedAt  int64
		enqueuedAt int64
		eligibleAt int64
		deadlineAt sql.NullInt64
		exitCode   sql.NullInt64
		retryExit  int
		retryTO    int
		st         string
	)
	err := sc.Scan(&j.ID, &j.IdempotencyKey, &j.ClientID, &st, &j.Priority, &j.Request.CPUMillis,
		&j.Request.MemoryBytes, &j.Image, &cmdJSON, &envJSON, &j.MaxAttempts, &j.AttemptCount,
		&timeoutNs, &retryExit, &retryTO, &createdAt, &updatedAt, &enqueuedAt, &eligibleAt,
		&deadlineAt, &j.CurrentAttemptID, &exitCode, &j.FailureClass, &j.Message, &j.RequestSpecHash)
	if err != nil {
		return nil, err
	}
	j.State = state.JobState(st)
	j.Timeout = time.Duration(timeoutNs)
	j.RetryOnProcessExit = retryExit != 0
	j.RetryOnTimeout = retryTO != 0
	j.CreatedAt = fromUnix(createdAt)
	j.UpdatedAt = fromUnix(updatedAt)
	j.EnqueuedAt = fromUnix(enqueuedAt)
	j.EligibleAt = fromUnix(eligibleAt)
	j.Deadline = fromUnixPtr(deadlineAt)
	j.ExitCode = toInt32Ptr(exitCode)
	_ = json.Unmarshal([]byte(cmdJSON), &j.Command)
	_ = json.Unmarshal([]byte(envJSON), &j.Env)
	return &j, nil
}

// InsertJob writes a new job row. The job must be in state SUBMITTED; the caller is
// expected to immediately TransitionJob it to QUEUED in the same transaction, which
// is what makes a half-submitted job impossible.
func (t *Tx) InsertJob(j *types.Job) error {
	if j.State != state.JobSubmitted {
		return fmt.Errorf("store: new jobs must start in SUBMITTED, got %s", j.State)
	}
	_, err := t.tx.Exec(`INSERT INTO jobs (`+jobColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.IdempotencyKey, j.ClientID, string(j.State), j.Priority, j.Request.CPUMillis,
		j.Request.MemoryBytes, j.Image, marshalJSON(j.Command), marshalJSON(j.Env), j.MaxAttempts,
		j.AttemptCount, int64(j.Timeout), boolToInt(j.RetryOnProcessExit), boolToInt(j.RetryOnTimeout),
		toUnix(j.CreatedAt), toUnix(j.UpdatedAt), toUnix(j.EnqueuedAt), toUnix(j.EligibleAt),
		toUnixPtr(j.Deadline), j.CurrentAttemptID, fromInt32Ptr(j.ExitCode), string(j.FailureClass),
		j.Message, j.RequestSpecHash)
	if err != nil {
		return fmt.Errorf("store: insert job: %w", err)
	}
	return t.appendTransition("job", j.ID, "", "", string(state.JobSubmitted), "submitted")
}

// GetJob loads one job by id.
func (t *Tx) GetJob(id string) (*types.Job, error) {
	row := t.tx.QueryRow(`SELECT `+jobColumns+` FROM jobs WHERE job_id = ?`, id)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get job %s: %w", id, err)
	}
	return j, nil
}

// GetJobByIdempotencyKey is the read half of idempotent submission.
func (t *Tx) GetJobByIdempotencyKey(key string) (*types.Job, error) {
	row := t.tx.QueryRow(`SELECT `+jobColumns+` FROM jobs WHERE idempotency_key = ?`, key)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get job by key: %w", err)
	}
	return j, nil
}

// SaveJob persists every mutable field except state. State changes must go through
// TransitionJob so that they are validated and audited.
func (t *Tx) SaveJob(j *types.Job) error {
	j.UpdatedAt = t.now
	_, err := t.tx.Exec(`UPDATE jobs SET
		priority=?, attempt_count=?, enqueued_at=?, eligible_at=?, deadline_at=?,
		current_attempt_id=?, exit_code=?, failure_class=?, message=?, updated_at=?
		WHERE job_id=?`,
		j.Priority, j.AttemptCount, toUnix(j.EnqueuedAt), toUnix(j.EligibleAt), toUnixPtr(j.Deadline),
		j.CurrentAttemptID, fromInt32Ptr(j.ExitCode), string(j.FailureClass), j.Message,
		toUnix(j.UpdatedAt), j.ID)
	if err != nil {
		return fmt.Errorf("store: save job %s: %w", j.ID, err)
	}
	return nil
}

// TransitionJob validates the edge, writes the new state, and appends to the audit
// log. It is the only way a job's state changes.
func (t *Tx) TransitionJob(j *types.Job, to state.JobState, reason string) error {
	next, err := state.Transition(j.State, to)
	if err != nil {
		return err
	}
	from := j.State
	j.State = next
	j.UpdatedAt = t.now
	if _, err := t.tx.Exec(`UPDATE jobs SET state=?, updated_at=? WHERE job_id=?`,
		string(next), toUnix(j.UpdatedAt), j.ID); err != nil {
		return fmt.Errorf("store: transition job %s: %w", j.ID, err)
	}
	return t.appendTransition("job", j.ID, j.CurrentAttemptID, string(from), string(next), reason)
}

// JobFilter narrows ListJobs.
type JobFilter struct {
	States   []state.JobState
	ClientID string
	Limit    int
	Offset   int
}

// ListJobs returns jobs newest first.
func (t *Tx) ListJobs(f JobFilter) ([]*types.Job, error) {
	q := `SELECT ` + jobColumns + ` FROM jobs`
	var args []any
	var where []string
	if len(f.States) > 0 {
		placeholders := ""
		for i, s := range f.States {
			if i > 0 {
				placeholders += ","
			}
			placeholders += "?"
			args = append(args, string(s))
		}
		where = append(where, "state IN ("+placeholders+")")
	}
	if f.ClientID != "" {
		where = append(where, "client_id = ?")
		args = append(args, f.ClientID)
	}
	for i, w := range where {
		if i == 0 {
			q += " WHERE " + w
		} else {
			q += " AND " + w
		}
	}
	q += " ORDER BY created_at DESC, job_id DESC"
	if f.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d OFFSET %d", f.Limit, f.Offset)
	}

	rows, err := t.tx.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list jobs: %w", err)
	}
	defer rows.Close()

	var out []*types.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// CountJobsInStates counts jobs across the given states, optionally for one client.
func (t *Tx) CountJobsInStates(clientID string, states ...state.JobState) (int, error) {
	if len(states) == 0 {
		return 0, nil
	}
	q := `SELECT COUNT(*) FROM jobs WHERE state IN (`
	args := make([]any, 0, len(states)+1)
	for i, s := range states {
		if i > 0 {
			q += ","
		}
		q += "?"
		args = append(args, string(s))
	}
	q += ")"
	if clientID != "" {
		q += " AND client_id = ?"
		args = append(args, clientID)
	}
	var n int
	if err := t.tx.QueryRow(q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: count jobs: %w", err)
	}
	return n, nil
}

// ---------------------------------------------------------------- attempts

const attemptColumns = `attempt_id, job_id, attempt_number, worker_id, state, lease_id,
	lease_expires_at, cpu_millis, memory_bytes, created_at, started_at, finished_at,
	exit_code, failure_class, message, stdout_tail, stderr_tail`

func scanAttempt(sc interface{ Scan(...any) error }) (*types.Attempt, error) {
	var (
		a          types.Attempt
		st         string
		leaseExp   int64
		createdAt  int64
		startedAt  sql.NullInt64
		finishedAt sql.NullInt64
		exitCode   sql.NullInt64
	)
	err := sc.Scan(&a.ID, &a.JobID, &a.Number, &a.WorkerID, &st, &a.LeaseID, &leaseExp,
		&a.Request.CPUMillis, &a.Request.MemoryBytes, &createdAt, &startedAt, &finishedAt,
		&exitCode, &a.FailureClass, &a.Message, &a.StdoutTail, &a.StderrTail)
	if err != nil {
		return nil, err
	}
	a.State = state.AttemptState(st)
	a.LeaseExpiresAt = fromUnix(leaseExp)
	a.CreatedAt = fromUnix(createdAt)
	a.StartedAt = fromUnixPtr(startedAt)
	a.FinishedAt = fromUnixPtr(finishedAt)
	a.ExitCode = toInt32Ptr(exitCode)
	return &a, nil
}

// InsertAttempt records a new physical execution.
func (t *Tx) InsertAttempt(a *types.Attempt) error {
	if a.State != state.AttemptAssigned {
		return fmt.Errorf("store: new attempts must start in ASSIGNED, got %s", a.State)
	}
	_, err := t.tx.Exec(`INSERT INTO attempts (`+attemptColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		a.ID, a.JobID, a.Number, a.WorkerID, string(a.State), a.LeaseID, toUnix(a.LeaseExpiresAt),
		a.Request.CPUMillis, a.Request.MemoryBytes, toUnix(a.CreatedAt), toUnixPtr(a.StartedAt),
		toUnixPtr(a.FinishedAt), fromInt32Ptr(a.ExitCode), string(a.FailureClass), a.Message,
		a.StdoutTail, a.StderrTail)
	if err != nil {
		return fmt.Errorf("store: insert attempt: %w", err)
	}
	return t.appendTransition("attempt", a.JobID, a.ID, "", string(state.AttemptAssigned), "assigned to "+a.WorkerID)
}

// GetAttempt loads one attempt.
func (t *Tx) GetAttempt(id string) (*types.Attempt, error) {
	row := t.tx.QueryRow(`SELECT `+attemptColumns+` FROM attempts WHERE attempt_id = ?`, id)
	a, err := scanAttempt(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get attempt %s: %w", id, err)
	}
	return a, nil
}

// ListAttemptsForJob returns a job's attempts in order.
func (t *Tx) ListAttemptsForJob(jobID string) ([]*types.Attempt, error) {
	rows, err := t.tx.Query(`SELECT `+attemptColumns+` FROM attempts WHERE job_id = ? ORDER BY attempt_number`, jobID)
	if err != nil {
		return nil, fmt.Errorf("store: list attempts: %w", err)
	}
	defer rows.Close()
	var out []*types.Attempt
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// SaveAttempt persists mutable fields other than state.
func (t *Tx) SaveAttempt(a *types.Attempt) error {
	_, err := t.tx.Exec(`UPDATE attempts SET
		lease_id=?, lease_expires_at=?, started_at=?, finished_at=?, exit_code=?,
		failure_class=?, message=?, stdout_tail=?, stderr_tail=? WHERE attempt_id=?`,
		a.LeaseID, toUnix(a.LeaseExpiresAt), toUnixPtr(a.StartedAt), toUnixPtr(a.FinishedAt),
		fromInt32Ptr(a.ExitCode), string(a.FailureClass), a.Message, a.StdoutTail, a.StderrTail, a.ID)
	if err != nil {
		return fmt.Errorf("store: save attempt %s: %w", a.ID, err)
	}
	return nil
}

// TransitionAttempt validates the edge, writes it, and audits it.
func (t *Tx) TransitionAttempt(a *types.Attempt, to state.AttemptState, reason string) error {
	next, err := state.TransitionAttempt(a.State, to)
	if err != nil {
		return err
	}
	from := a.State
	a.State = next
	if _, err := t.tx.Exec(`UPDATE attempts SET state=? WHERE attempt_id=?`, string(next), a.ID); err != nil {
		return fmt.Errorf("store: transition attempt %s: %w", a.ID, err)
	}
	return t.appendTransition("attempt", a.JobID, a.ID, string(from), string(next), reason)
}

// FinishAttempt moves an attempt to a terminal state and releases the worker's
// reservation in the same transaction.
//
// Resource release lives here, rather than in the scheduler, so that it is impossible
// to terminate an attempt without freeing its capacity. Because terminal states are
// absorbing, a second call fails the transition and the release cannot run twice.
func (t *Tx) FinishAttempt(a *types.Attempt, to state.AttemptState, reason string) error {
	if !to.IsTerminal() {
		return fmt.Errorf("store: FinishAttempt requires a terminal state, got %s", to)
	}
	if err := t.TransitionAttempt(a, to, reason); err != nil {
		return err
	}
	if a.FinishedAt == nil {
		now := t.now
		a.FinishedAt = &now
	}
	if err := t.SaveAttempt(a); err != nil {
		return err
	}
	w, err := t.GetWorker(a.WorkerID)
	if errors.Is(err, ErrNotFound) {
		// A worker row can be absent only if it was purged; the attempt still
		// has to terminate, and there is no allocation left to release.
		return nil
	}
	if err != nil {
		return err
	}
	w.Allocated = w.Allocated.Sub(a.Request)
	return t.SaveWorker(w)
}

// ExpiredAttempts returns live attempts whose lease has already lapsed.
func (t *Tx) ExpiredAttempts(now time.Time, limit int) ([]*types.Attempt, error) {
	rows, err := t.tx.Query(`SELECT `+attemptColumns+` FROM attempts
		WHERE state IN (?, ?) AND lease_expires_at <= ? ORDER BY lease_expires_at LIMIT ?`,
		string(state.AttemptAssigned), string(state.AttemptRunning), now.UnixNano(), limit)
	if err != nil {
		return nil, fmt.Errorf("store: expired attempts: %w", err)
	}
	defer rows.Close()
	var out []*types.Attempt
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// LiveAttempts returns every attempt that is not terminal, optionally for one worker.
func (t *Tx) LiveAttempts(workerID string) ([]*types.Attempt, error) {
	q := `SELECT ` + attemptColumns + ` FROM attempts WHERE state IN (?, ?)`
	args := []any{string(state.AttemptAssigned), string(state.AttemptRunning)}
	if workerID != "" {
		q += " AND worker_id = ?"
		args = append(args, workerID)
	}
	rows, err := t.tx.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: live attempts: %w", err)
	}
	defer rows.Close()
	var out []*types.Attempt
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------- workers

const workerColumns = `worker_id, hostname, version, labels, cpu_capacity, memory_capacity,
	cpu_allocated, memory_allocated, state, registered_at, last_heartbeat_at, generation`

func scanWorker(sc interface{ Scan(...any) error }) (*types.Worker, error) {
	var (
		w          types.Worker
		labelsJSON string
		st         string
		registered int64
		heartbeat  int64
	)
	err := sc.Scan(&w.ID, &w.Hostname, &w.Version, &labelsJSON, &w.Capacity.CPUMillis,
		&w.Capacity.MemoryBytes, &w.Allocated.CPUMillis, &w.Allocated.MemoryBytes, &st,
		&registered, &heartbeat, &w.Generation)
	if err != nil {
		return nil, err
	}
	w.State = state.WorkerState(st)
	w.RegisteredAt = fromUnix(registered)
	w.LastHeartbeatAt = fromUnix(heartbeat)
	_ = json.Unmarshal([]byte(labelsJSON), &w.Labels)
	return &w, nil
}

// UpsertWorker inserts a worker or updates its registration details, bumping the
// generation. Allocation is not touched: a re-registering worker's existing
// reservations stay booked until the scheduler reclaims their leases.
func (t *Tx) UpsertWorker(w *types.Worker) error {
	_, err := t.tx.Exec(`INSERT INTO workers (`+workerColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(worker_id) DO UPDATE SET
			hostname=excluded.hostname, version=excluded.version, labels=excluded.labels,
			cpu_capacity=excluded.cpu_capacity, memory_capacity=excluded.memory_capacity,
			state=excluded.state, last_heartbeat_at=excluded.last_heartbeat_at,
			generation=workers.generation+1`,
		w.ID, w.Hostname, w.Version, marshalJSON(w.Labels), w.Capacity.CPUMillis,
		w.Capacity.MemoryBytes, w.Allocated.CPUMillis, w.Allocated.MemoryBytes, string(w.State),
		toUnix(w.RegisteredAt), toUnix(w.LastHeartbeatAt), w.Generation)
	if err != nil {
		return fmt.Errorf("store: upsert worker %s: %w", w.ID, err)
	}
	return nil
}

// GetWorker loads one worker.
func (t *Tx) GetWorker(id string) (*types.Worker, error) {
	row := t.tx.QueryRow(`SELECT `+workerColumns+` FROM workers WHERE worker_id = ?`, id)
	w, err := scanWorker(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get worker %s: %w", id, err)
	}
	return w, nil
}

// SaveWorker persists a worker, refusing writes that would violate invariant I2.
// Enforcing capacity here means no caller can oversubscribe a worker by accident:
// the transaction simply does not commit.
func (t *Tx) SaveWorker(w *types.Worker) error {
	if w.Allocated.CPUMillis < 0 || w.Allocated.MemoryBytes < 0 {
		return fmt.Errorf("%w: worker %s allocation negative (cpu=%d mem=%d)",
			ErrCapacityExceeded, w.ID, w.Allocated.CPUMillis, w.Allocated.MemoryBytes)
	}
	if !w.Allocated.Fits(w.Capacity) {
		return fmt.Errorf("%w: worker %s allocated cpu=%d/%d mem=%d/%d",
			ErrCapacityExceeded, w.ID, w.Allocated.CPUMillis, w.Capacity.CPUMillis,
			w.Allocated.MemoryBytes, w.Capacity.MemoryBytes)
	}
	_, err := t.tx.Exec(`UPDATE workers SET hostname=?, version=?, labels=?, cpu_capacity=?,
		memory_capacity=?, cpu_allocated=?, memory_allocated=?, state=?, last_heartbeat_at=?,
		generation=? WHERE worker_id=?`,
		w.Hostname, w.Version, marshalJSON(w.Labels), w.Capacity.CPUMillis, w.Capacity.MemoryBytes,
		w.Allocated.CPUMillis, w.Allocated.MemoryBytes, string(w.State), toUnix(w.LastHeartbeatAt),
		w.Generation, w.ID)
	if err != nil {
		return fmt.Errorf("store: save worker %s: %w", w.ID, err)
	}
	return nil
}

// ListWorkers returns the whole fleet.
func (t *Tx) ListWorkers() ([]*types.Worker, error) {
	rows, err := t.tx.Query(`SELECT ` + workerColumns + ` FROM workers ORDER BY worker_id`)
	if err != nil {
		return nil, fmt.Errorf("store: list workers: %w", err)
	}
	defer rows.Close()
	var out []*types.Worker
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// DeleteWorker removes a worker row. Only safe once it holds no live attempts.
func (t *Tx) DeleteWorker(id string) error {
	_, err := t.tx.Exec(`DELETE FROM workers WHERE worker_id = ?`, id)
	return err
}

// ExecRawForTesting runs arbitrary SQL inside the transaction, bypassing the state
// machine and the capacity checks.
//
// It exists for one purpose: the invariant checker's own tests need to construct
// corruption that a correct scheduler cannot produce. A checker that has only ever
// seen valid states is not evidence that it would catch an invalid one.
func (t *Tx) ExecRawForTesting(query string, args ...any) error {
	if !t.write {
		return errors.New("store: ExecRawForTesting requires a write transaction")
	}
	_, err := t.tx.Exec(query, args...)
	return err
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
