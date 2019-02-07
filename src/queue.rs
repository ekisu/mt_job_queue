extern crate interval;
extern crate gcollections;

use interval::interval_set::IntervalSet;
use gcollections::ops::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Sender, Receiver, channel};
use std::sync::{Mutex, Arc};
use std::thread;
use std::thread::JoinHandle;

struct Worker {
    handle: JoinHandle<()>
}

impl Worker {
    fn new<JobArgument, JobResult, F>(
        job_rx: Arc<Mutex<Receiver<(usize, JobArgument)>>>,
        results_tx: Arc<Mutex<Sender<(usize, JobResult)>>>,
        process_job_fn: &'static F
    ) -> Self
    where
        JobArgument: Send + 'static,
        JobResult: Send + 'static,
        F: Fn(JobArgument) -> JobResult + Sync + Send {
        let handle = thread::spawn(move || {
            loop {
                // FIXME maybe actually care for errors?
                let (job_id, job_argument) = job_rx.lock().unwrap().recv().unwrap();
                let result = process_job_fn(job_argument);
                results_tx.lock().unwrap().send((job_id, result)).unwrap();
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
    complete_jobs: Arc<Mutex<IntervalSet<usize>>>,
    last_job_id: AtomicUsize
}

fn estimate_position(complete_jobs: &IntervalSet<usize>, job_id: usize) -> usize {
    complete_jobs.complement().shrink_right(job_id).size()
}

impl<JobArgument> Queue<JobArgument>
where
    JobArgument: Send + 'static {
    pub fn new<JobResult, F, C>(num_workers: usize, process_job_fn: &'static F, on_job_completed_fn: &'static C) -> Self
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
            Worker::new(job_rx.clone(), results_tx.clone(), process_job_fn)
        }).collect();

        let complete_jobs = Arc::new(Mutex::new(IntervalSet::empty()));
        let _complete_jobs = complete_jobs.clone();

        thread::spawn(move || {
            loop {
                let (job_id, job_result) = results_rx.lock().unwrap().recv().unwrap();
                {
                    let mut _guard = _complete_jobs.lock().unwrap();
                    *_guard = _guard.union(&IntervalSet::singleton(job_id));
                }
                on_job_completed_fn(job_result)
            }
        });

        Queue {
            job_tx, workers, complete_jobs,
            last_job_id: AtomicUsize::new(0)
        }
    }

    pub fn enqueue(&self, job_argument: JobArgument) {
        let _job_tx = self.job_tx.lock().unwrap();
        _job_tx.send((self.last_job_id.fetch_add(1, Ordering::SeqCst), job_argument)).unwrap();
    }

    pub fn position(&self, job_id: usize) -> usize {
        // Really makes me think.
        estimate_position(&*self.complete_jobs.lock().unwrap(), job_id)
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
