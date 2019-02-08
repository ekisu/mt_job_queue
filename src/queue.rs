extern crate interval;
extern crate gcollections;

use interval::interval_set::IntervalSet;
use gcollections::ops::*;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Sender, Receiver, channel};
use std::sync::{Mutex, Arc};
use std::thread;
use std::thread::JoinHandle;

enum ProgressUpdate<JobResult> {
    Acknowledgement(usize),
    Complete(usize, JobResult)
}

struct Worker {
    handle: JoinHandle<()>
}

impl Worker {
    fn new<JobArgument, JobResult, F>(
        job_rx: Arc<Mutex<Receiver<(usize, JobArgument)>>>,
        progress_tx: Arc<Mutex<Sender<ProgressUpdate<JobResult>>>>,
        process_job_fn: Arc<F>
    ) -> Self
    where
        JobArgument: Send + 'static,
        JobResult: Send + 'static,
        F: Fn(JobArgument) -> JobResult + Sync + Send + 'static {
        let handle = thread::spawn(move || {
            use ProgressUpdate::*;
            loop {
                // FIXME maybe actually care for errors?
                let (job_id, job_argument) = job_rx.lock().unwrap().recv().unwrap();
                progress_tx.lock().unwrap().send(Acknowledgement(job_id)).unwrap();

                let result = process_job_fn(job_argument);
                progress_tx.lock().unwrap().send(Complete(job_id, result)).unwrap();
            }
        });
        
        Worker {
            handle
        }
    }
}

pub struct Queue<JobArgument> {
    job_tx: Arc<Mutex<Sender<(usize, JobArgument)>>>,
    //results_rx: Arc<Mutex<Receiver<(usize, JobResult)>>>,
    workers: Vec<Worker>,
    acknowledged_jobs: Arc<Mutex<BTreeSet<usize>>>,
    complete_jobs: Arc<Mutex<IntervalSet<usize>>>,
    last_job_id: AtomicUsize
}

fn estimate_position(complete_jobs: &IntervalSet<usize>, job_id: usize) -> usize {
    complete_jobs.complement().shrink_right(job_id).size()
}

pub enum JobState {
    Pending,
    Acknowledged,
    Complete
}

impl<JobArgument> Queue<JobArgument>
where
    JobArgument: Send + 'static {
    pub fn new<JobResult, F, C>(num_workers: usize, process_job_fn: Arc<F>, on_job_completed_fn: Arc<C>) -> Self
    where
        JobResult: Send + 'static,
        F: Fn(JobArgument) -> JobResult + Sync + Send + 'static,
        C: Fn(JobResult) -> () + Sync + Send + 'static {
        let (_job_tx, _job_rx) = channel();
        let (_results_tx, _results_rx) = channel();

        let job_tx = Arc::new(Mutex::new(_job_tx));
        let job_rx = Arc::new(Mutex::new(_job_rx));
        let results_tx = Arc::new(Mutex::new(_results_tx));
        let results_rx = Arc::new(Mutex::new(_results_rx));

        let workers : Vec<_> = (0..num_workers).map(|_| {
            Worker::new(job_rx.clone(), results_tx.clone(), process_job_fn.clone())
        }).collect();

        let acknowledged_jobs = Arc::new(Mutex::new(BTreeSet::new()));
        let _acknowledged_jobs = acknowledged_jobs.clone();

        let complete_jobs = Arc::new(Mutex::new(IntervalSet::empty()));
        let _complete_jobs = complete_jobs.clone();

        thread::spawn(move || {
            loop {
                let update = results_rx.lock().unwrap().recv().unwrap();
                match update {
                    ProgressUpdate::Acknowledgement(job_id) => {
                        _acknowledged_jobs.lock().unwrap().insert(job_id);
                    },
                    ProgressUpdate::Complete(job_id, result) => {
                        {
                            // Always lock both in this order: ack -> complete
                            let mut _guard_ack = _acknowledged_jobs.lock().unwrap();
                            let mut _guard_complete = _complete_jobs.lock().unwrap();

                            _guard_ack.remove(&job_id);
                            *_guard_complete = _guard_complete.union(&IntervalSet::singleton(job_id));
                        }
                        on_job_completed_fn(result);
                    }
                };
            }
        });

        Queue {
            job_tx, workers, acknowledged_jobs, complete_jobs,
            last_job_id: AtomicUsize::new(0)
        }
    }

    pub fn enqueue(&self, job_argument: JobArgument) -> usize {
        let _job_tx = self.job_tx.lock().unwrap();
        let job_id = self.last_job_id.fetch_add(1, Ordering::SeqCst);
        _job_tx.send((job_id, job_argument)).unwrap();
        job_id
    }

    pub fn position(&self, job_id: usize) -> usize {
        // Really makes me think.
        estimate_position(&*self.complete_jobs.lock().unwrap(), job_id)
    }

    pub fn job_state(&self, job_id: usize) -> JobState {
        // Always lock both in this order: ack -> complete
        let mut _guard_ack = self.acknowledged_jobs.lock().unwrap();
        let mut _guard_complete = self.complete_jobs.lock().unwrap();

        if _guard_complete.contains(&job_id) {
            JobState::Complete
        } else if _guard_ack.contains(&job_id) {
            JobState::Acknowledged
        } else {
            JobState::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use interval::interval_set::{IntervalSet, ToIntervalSet};

    #[test]
    fn test_no_jobs_completed() {
        let no_jobs : IntervalSet<usize> = IntervalSet::empty();
        assert_eq!(estimate_position(&no_jobs, 0), 1);
    }

    #[test]
    fn test_few_jobs_completed() {
        // Jobs 0, 1, 2 and 3 have completed.
        let few_jobs_completed : IntervalSet<usize> = vec![(0, 3)].to_interval_set();
        assert_eq!(estimate_position(&few_jobs_completed, 6), 3);
    }

    #[test]
    fn test_jobs_with_hole() {
        // Jobs 0, 1, and 3 have completed.
        let jobs_with_hole : IntervalSet<usize> = vec![(0, 1), (3, 3)].to_interval_set();
        assert_eq!(estimate_position(&jobs_with_hole, 6), 4);
    }

    #[test]
    fn test_before_after_completion() {
        // Jobs 0 and 1 have completed.
        let mut jobs : IntervalSet<usize> = vec![(0, 1)].to_interval_set();
        assert_eq!(estimate_position(&jobs, 6), 5);

        jobs = jobs.union(&IntervalSet::singleton(3));
        assert_eq!(estimate_position(&jobs, 6), 4);
    }
}
