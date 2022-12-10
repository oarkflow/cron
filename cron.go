package cron

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries   []*Entry
	chain     Chain
	stop      chan struct{}
	add       chan *Entry
	remove    chan EntryID
	snapshot  chan chan []Entry
	running   bool
	logger    Logger
	runningMu sync.Mutex
	location  *time.Location
	parser    ScheduleParser
	nextID    EntryID
	jobWaiter sync.WaitGroup
	start     chan EntryID
	pause     chan EntryID
	doJob     chan EntryID
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run(ctx context.Context) error
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	Title string

	Spec string

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job `json:"-"`

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	Job Job `json:"-"`

	Enable bool
	Done   time.Time
	Fail   time.Time
	Logs   []string
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//	Time Zone
//	  Description: The time zone in which schedules are interpreted
//	  Default:     time.Local
//
//	Parser
//	  Description: Parser converts cron spec strings into cron.Schedules.
//	  Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//	Chain
//	  Description: Wrap submitted jobs to customize behavior.
//	  Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:   nil,
		chain:     NewChain(),
		add:       make(chan *Entry),
		stop:      make(chan struct{}),
		snapshot:  make(chan chan []Entry),
		remove:    make(chan EntryID),
		start:     make(chan EntryID, 1),
		pause:     make(chan EntryID, 1),
		doJob:     make(chan EntryID, 1),
		running:   false,
		runningMu: sync.Mutex{},
		logger:    DefaultLogger,
		location:  time.Local,
		parser:    standardParser,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func(context.Context) error

func (f FuncJob) Run(ctx context.Context) error { return f(ctx) }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(title, spec string, cmd func(context.Context) error) (EntryID, error) {
	return c.AddJob(title, spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(title, spec string, cmd Job) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.schedule(title, spec, schedule, cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(title string, schedule Schedule, cmd Job) EntryID {
	return c.schedule(title, "", schedule, cmd)
}

// schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) schedule(title string, spec string, schedule Schedule, cmd Job) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.nextID++
	entry := &Entry{
		Enable:     true,
		Title:      title,
		Spec:       spec,
		ID:         c.nextID,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
		Logs:       []string{},
	}
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		c.add <- entry
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		replyChan := make(chan []Entry, 1)
		c.snapshot <- replyChan
		return <-replyChan
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

func (c *Cron) StartEntry(id EntryID) {
	c.start <- id
}

func (c *Cron) PauseEntry(id EntryID) {
	c.pause <- id
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start(ctx context.Context) error {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return nil
	}
	c.running = true
	go c.run(ctx)
	return nil
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run(ctx context.Context) error {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return nil
	}
	c.running = true
	c.runningMu.Unlock()
	return c.run(ctx)
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run(ctx context.Context) error {
	c.logger.Info("start")

	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
		c.logger.Info("schedule", "now", now, "entry", entry.ID, "title", entry.Title, "next", entry.Next)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case now = <-timer.C:
				now = now.In(c.location)
				c.logger.Info("wake", "now", now)

				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if !e.Enable {
						continue
					}
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					c.runEntry(ctx, e)
					e.Prev = e.Next
					e.Next = e.Schedule.Next(now)
					c.logger.Info("run", "now", now, "entry", e.ID, "title", e.Title, "next", e.Next)
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)
				c.logger.Info("added", "now", now, "entry", newEntry.ID, "title", newEntry.Title, "next", newEntry.Next)

			case replyChan := <-c.snapshot:
				replyChan <- c.entrySnapshot()
				continue

			case <-c.stop:
				timer.Stop()
				c.logger.Info("stop")
				return nil

			case id := <-c.remove:
				timer.Stop()
				now = c.now()
				c.removeEntry(id)
				c.logger.Info("removed", "entry", id)

			case id := <-c.pause:
				c.pauseEntry(id)
				c.logger.Info("pause", "entry", id)

			case id := <-c.start:
				c.startEntry(id)
				c.logger.Info("start", "entry", id)

			case id := <-c.doJob:
				for _, e := range c.entries {
					if e.ID == id {
						nw := c.now()
						c.runEntry(ctx, e)
						e.Prev = nw
						e.Next = e.Schedule.Next(nw)
						c.logger.Info("run", "now", now, "entry", e.ID, "title", e.Title, "next", e.Next)
					}
				}

			}

			break
		}
	}
}

func (c *Cron) RunEntry(id EntryID) {
	c.doJob <- id
}

var maxLogs = 10

// runEntry runs the given job in a new goroutine.
func (c *Cron) runEntry(ctx context.Context, e *Entry) {
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()
		err := e.WrappedJob.Run(ctx)
		now := c.now()
		if err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Error(err, "job run err")
			if len(e.Logs) >= maxLogs {
				copy(e.Logs[0:], e.Logs[1:maxLogs])
				e.Logs[maxLogs-1] = fmt.Sprintf("%v %v", now, err)
				return
			}

			e.Logs = append(e.Logs, fmt.Sprintf("%v %v", now, err))
			e.Fail = now
		} else {
			e.Done = now
		}
	}()
}

// // startJob runs the given job in a new goroutine.
// func (c *Cron) startJob(j Job) {
// 	c.jobWaiter.Add(1)
// 	go func() {
// 		defer c.jobWaiter.Done()
// 		err := j.Run()
// 		if err != nil {
// 			c.logger.Error(err, "job run err")
// 		}
// 	}()
// }

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop(ctx context.Context) error {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.stop <- struct{}{}
		c.running = false
	}
	// go func() {
	c.jobWaiter.Wait()
	// }()
	return nil
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	var entries = make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	return entries
}

func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}

func (c *Cron) pauseEntry(id EntryID) {
	for _, e := range c.entries {
		if e.ID == id {
			e.Enable = false
			return
		}
	}
}

func (c *Cron) startEntry(id EntryID) {
	for _, e := range c.entries {
		if e.ID == id {
			e.Enable = true
			e.Next = e.Schedule.Next(c.now())
			return
		}
	}
}
