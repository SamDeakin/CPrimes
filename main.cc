#include <iostream>
#include <math.h>
#include <thread>
#include <queue>

#include <tbb/concurrent_unordered_map.h>

using namespace std;

class GlobalInfo;
GlobalInfo globals;

// Tuning variables
size_t NUM_THREADS = 1;
size_t MAX_CHECKPOINT_SIZE_PER_THREAD = 20000;
#define MAX_CHECKPOINT_SIZE (NUM_THREADS * MAX_CHECKPOINT_SIZE_PER_THREAD)

// TODO when finished disable extra logic and check:
// Checkpoint updates
// Number processing
// Number's processed into right checkpoints

/**
 * Contains info used by all threads
 * We use a concurrent version of std::unordered_map to avoid complicated locking.
 */
struct GlobalInfo {
    uint64_t max_range;
    uint64_t start_range;
    uint64_t max_try;

    /**
     * It is not required to hold a lock to insert or read result_table, but erasing from it is not thread_safe.
     * Generally, all other threads should be waiting on cv, then result_table should be swapped for a new one.
     * To check if a thread is waiting, check ThreadContext.done_checkpoint, as it will wait after setting that.
     */
    tbb::concurrent_unordered_map<uint64_t,bool> *result_table;

    condition_variable cv;

    /**
     * All_queued is true when the main thread is done queueing numbers for this checkpoint. Threads should wait on
     * cv when they have finished their queue and this is true.
     * The mtx should be held before accessing all_queued. To prevent deadlock, it should be considered an invalid
     * behavior to acquire a ThreadContext mtx after acquiring this one.
     */
    mutex mtx;
    bool all_queued;

    uint64_t checkpoint[4];

    /**
     * primes_table should not be modified, only read from to determine if a certain number is prime.
     */
    const tbb::concurrent_unordered_map<uint64_t,bool> *primes_table;

    /**
     * This is just a helper to avoid rewriting this code elsewhere.
     * The runtime of this will be average O(1), worst O(N), but this is an ok helper because
     * threads will not need to access the values in the table afterwords, just know whether the value
     * exists or is true. Still, threads should cache this value rather than calling it multiple times.
     */
    bool is_prime(uint64_t number) {
        auto it = this->result_table->find(number);
        return it == this->result_table->end() || it->second;
    }

    GlobalInfo() : all_queued(false) {}

    uint64_t update_checkpoint() {
        // First odd number greater than last checkpoint
        uint64_t end_num = this->checkpoint[0] + MAX_CHECKPOINT_SIZE;
        end_num = end_num + 1 - (end_num % 2); // First odd number greater than last checkpoint

        // This is ok cause theres only 4 values. Avoid branching in a for loop.
        this->checkpoint[3] = this->checkpoint[2];
        this->checkpoint[2] = this->checkpoint[1];
        this->checkpoint[1] = this->checkpoint[0];
        this->checkpoint[0] = min(end_num, this->max_range + 1); // Include max_range in this range
    }
};


/**
 * Contains info local to a thread; Each thread should have it's own.
 * A thread must hold the lock before reading or modifying anything.
 *
 * We introduce producer-consumer semantics to this object:
 * Producer:
 *      Will acquire mtx
 *      add to the queue
 *      notify all waiting on cv
 *      release mtx
 * Consumer:
 *      acquire mtx
 *      pop from queue
 *      if none are in queue and exit_when_finished is false then wait on cv
 *      release mtx
 *      process all popped from queue
 */
class ThreadContext {
public:
    mutex mtx;
    condition_variable cv;

    queue<uint64_t> q;

    /**
     * done_checkpoint will be set when the thread has finished all internally held queues
     * and processed one number larger than checkpoint.
     */
    bool done_checkpoint;

    ThreadContext() : done_checkpoint(false) {}

    /**
     * The main thread loop for this program.
     * info is the local ThreadContext object for this thread.
     *
     * The loop essentially does three things over and over until the main thread exits and is finished.
     *
     * TODO finish this comment
     */
    void run() {
        bool local_done_checkpoint = false;

        queue<uint64_t> *done_to_checkpoint = new queue<uint64_t>();
        queue<uint64_t> *prime = new queue<uint64_t>();

        while(true) {
            uint64_t next;

            {
                /**
                 * Just a block to hold our lock ~music notes emoji~
                 */
                // TODO redo waiting logic
                unique_lock<mutex> lk(this->mtx);
                // TODO Threads should exit when done the last checkpoint

                while(this->q.empty() && !this->exit_when_finished) {
                    this->cv.wait(lk);
                }

                if (this->q.empty() && this->exit_when_finished) {
                    break;
                }

                next = this->q.front();
                this->q.pop();
            }

            if (local_done_checkpoint && globals.checkpoint[0] == globals.max_try) {
                /**
                 * In this step we wait for the main thread to any atomic operations on globals then signal the checkpoint
                 * has been updated.
                 */
                unique_lock<mutex> lk(this->mtx);
                this->done_checkpoint = true;
                globals.cv.wait(lk);
                this->done_checkpoint = false);
            }

            // Since next is a new number, process from start_range to the checkpoint
            this->process(next, globals.start_range, globals.checkpoint[0]);

            // TODO add next to done_to_checkpoint (maybe if local_checkpoint != max_try)

            // Check for updated checkpoint
            if (checkpoint != local_checkpoint && !local_done_checkpoint) {
                // TODO process held queues then update checkpoint
                // After processing: If local_checkpoint == max_try then don't requeue number?
                // After processing: test primeness and put into primes if < local_last_checkpoint

                local_done_checkpoint = true;
            }

            // TODO
            // Make sure thread's don't immediately start up then wait on a lock that main won't signal
            // Make sure it's impossible for run to lock on empty queue before checkpoint is updated
            // Make sure when first checkpoint == max_try thread doesn't exit before receiving queue stuff
        }

        // TODO make sure prime and done_to_checkpoint get drained
        // TODO Clean up thread
        delete done_to_checkpoint;
        delete prime;
    }

private:
    /**
     * Processes num, adding info to globals.result_table
     */
    void process(uint64_t num, uint64_t start_num, uint64_t checkpoint) {
        uint64_t interval = 2 * num;

        // Start never goes below 0 or funky stuff happens
        // Always start at least at 2 * num
        // TODO Should this actually start at num instead of 2num?
        uint64_t start = (start_num / interval) * interval + interval;
        if (start % 2 == 0) {
            start += num;
        }

        for (uint64_t i = start; i < checkpoint; i = i + interval) {
            (*globals.result_table)[i] = false;
        }
    }
};


/**
 * Iterates from start to end, flushing any primes found to stdout as results
 */
void flush_primes(const tbb::concurrent_unordered_map<uint64_t,bool> *results, uint64_t start, uint64_t end) {
    if (start % 2 == 0) {
        if (start == 2) {
            // The one edge case
            cout << start << endl;
        }
        start++;
    }
    for (uint64_t i = start; i < end; i = i + 2) {
        auto it = results->find(i);
        if (it == results->end() || it->second) {
            cout << i << endl;
        }
    }
}

/**
 * Wait for all threads to be done processing up to the current checkpoint
 */
void wait_for_threads(ThreadContext *contexts) {
    for (size_t i = 0; i < NUM_THREADS; i++) {
        unique_lock<mutex> lk(contexts[i].mtx);
        if (!contexts[i].done_checkpoint) {
            // If this thread isn't done, wait for it to finish
            contexts[i].cv.wait(lk);
        }
    }
}


int main() {
    uint64_t last_queued;

    globals.start_range = 2;
    globals.max_range = 10000000000;
    // 2 * because for large numbers we could experience a floating point error, but by less than a factor of max_try
    globals.max_try = 2 * uint64_t(sqrtl(globals.max_range));

    // Set up checkpoints
    globals.checkpoint[3] = globals.start_range;
    globals.checkpoint[2] = globals.start_range;
    globals.checkpoint[1] = globals.start_range;
    globals.checkpoint[0] = globals.start_range;
    globals.update_checkpoint();

    globals.result_table = NULL;
    globals.primes_table = NULL;
    const tbb::concurrent_unordered_map<uint64_t,bool> *results = NULL; // Results created by the threads

    // Split into threads
    thread threads[NUM_THREADS];
    ThreadContext contexts[NUM_THREADS]; // Relying on default initialization
    for (size_t i = 0; i < NUM_THREADS; i++) {
        threads[i] = thread(contexts[i].run, &contexts[i]);
    }

    while (globals.checkpoint[0] < globals.max_range) {

        // All Threads are done by here
        {
            unique_lock<mutex> lk(globals.mtx);

            // We process this table later in this iteration
            results = globals.primes_table;
            globals.primes_table = globals.result_table;
            globals.result_table = new tbb::concurrent_unordered_map<uint64_t,bool>();

            globals.update_checkpoint();
            globals.all_queued = false;
            globals.cv.notify_all();
        }

        size_t next_thread = NUM_THREADS;
        for (
                uint64_t i = globals.checkpoint[1]; // Start at end of last checkpoint
                i < globals.checkpoint[0]; // Iterate over every number less than current checkpoint
                i = i * 2 // Iterate over odd numbers
                ) {
            next_thread = (next_thread + 1) % NUM_THREADS; // We do this first so it happens outside of the lock
            unique_lock<mutex> lk(contexts[next_thread].mtx);
            contexts[next_thread].q.push(i);
            contexts[next_thread].cv.notify_all();
        }

        {
            unique_lock<mutex> lk(globals.mtx);
            globals.all_queued = true;
        }

        // Results will be NULL on the first two loops
        if (results != NULL) {
            // Flush results to stdout so we can free up some memory
            // We iterate over the second-last to third-last checkpoints
            flush_primes(results, globals.checkpoint[3], globals.checkpoint[2]);
            delete results;
            results = NULL;
        }

        // Skip this step on the last iteration
        if (globals.checkpoint[0] < globals.max_range) {
            /**
             * Wait for every thread to finish this checkpoint
             * We don't wait on the last iteration because we want to do some extra calculations
             * and thread's won't wait, they'll exit instead.
             */
            wait_for_threads(contexts);
        }
    }

    /*
     * At this point:
     *      results == NULL
     *      globals.primes_table contains data between checkpoints[2] and checkpoints[1] and still in use
     *      globals.result_table is not done being created yet
     */

    // iterate over the second-last to third-last checkpoints here
    flush_primes(globals.result_table, globals.checkpoint[2], globals.checkpoint[1]);

    // Wait for threads to finish. They will exit this time instead of waiting
    for (size_t i = 0; i < NUM_THREADS; i++) {
        threads[i].join();
    }

    // primes_table will be NULL if there was only one checkpoint
    if (globals.primes_table != NULL) {
        // Threads are done accessing primes_table now
        delete globals.primes_table;
        globals.primes_table = NULL;
    }

    // Now it's safe to iterate over result_table
    flush_primes(globals.result_table, globals.checkpoint[1], globals.checkpoint[0]);
    delete globals.result_table;
    globals.result_table = NULL;
}
