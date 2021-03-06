#include <iostream>
#include <math.h>
#include <thread>
#include <queue>

#include <tbb/concurrent_unordered_map.h>

using namespace std;

// Tuning variables
size_t NUM_THREADS = 1;
size_t MAX_CHECKPOINT_SIZE_PER_THREAD = 20;
#define MAX_CHECKPOINT_SIZE (NUM_THREADS * MAX_CHECKPOINT_SIZE_PER_THREAD)

// TODO when finished disable extra logic and check:
// Checkpoint updates - Good
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
     * behavior to attempt to acquire this lock while holding a ThreadContext Lock.
     */
    mutex mtx;
    volatile uint64_t last_queued;

    volatile uint64_t checkpoint[4];

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

    GlobalInfo() : last_queued(0) {}

    void update_checkpoint() {
        // First odd number greater than last checkpoint
        uint64_t end_num = this->checkpoint[0] + MAX_CHECKPOINT_SIZE;
        end_num = end_num + 1 - (end_num % 2); // First odd number greater than last checkpoint

        // This is ok cause theres only 4 values. Avoid branching in a for loop.
        this->checkpoint[3] = this->checkpoint[2];
        this->checkpoint[2] = this->checkpoint[1];
        this->checkpoint[1] = this->checkpoint[0];
        this->checkpoint[0] = min(end_num, this->max_range + 1); // Include max_range in this range

        cout << "Check:";
        for (int i = 0; i < 4; i++) {
            cout << " " << this->checkpoint[i];
        }
        cout << endl;
    }
};
// This is bad but it has to be accessible from all threads
// So many better ways to do this
GlobalInfo globals;


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
    volatile bool done_checkpoint;

    ThreadContext() : done_checkpoint(false) {}

    /**
     * The main thread loop for this program.
     * info is the local ThreadContext object for this thread.
     *
     * This is a high level description of what the loop does:
     *  First drains prime and adds items to a new prime
     *      This is first because items from done_to_checkpoint will be added to the new prime
     *  Second drains done_to_checkpoint
     *      Items will be discarded and not processed if they are not prime
     *  Third threads will drain their queue and then check if they should wait for others to finish the checkpoint
     *      Threads will acquire their lock then check the queue for the next number
     *      Threads then release their lock
     *      If there is a number they will process that number, then repeat
     *      If there is no number they will acquire the global lock and check if the checkpoint is finished queueing
     *      If it is finished queueing the thread proceeds to step 4
     *      If it is not finished queueing the thread waits on it's own cv
     *  Fourth threads will check if the current checkpoint is the last checkpoint
     *      Exit if it's the last checkpoint
     *      Repeat from step 1 if it's not
     */
    void run() {
        done_to_checkpoint = new queue<uint64_t>();
        prime = new queue<uint64_t>();

        do {
            // First figure out the range to process over this iteration
            uint64_t start_num = globals.checkpoint[1];
            uint64_t checkpoint = globals.checkpoint[0];

            // Step one
            drain_prime(start_num, checkpoint);

            // Step two
            drain_done_to_checkpoint(start_num, checkpoint);

            // Wait here if the global checkpoint hasn't been updated yet
            {
                unique_lock<mutex> lk(globals.mtx);
                cout << "Thread speed check: " << globals.last_queued << " " << last_updated_checkpoint << endl;
                if (globals.last_queued == last_updated_checkpoint) {
                    globals.cv.wait(lk);
                }
            }

            // Step three
            process_queue(start_num, checkpoint);

            // Update done_checkpoint to signal we are done the queue
            cout << "Thread done" << endl;
            {
                unique_lock<mutex> lk(mtx);
                done_checkpoint = true;
                last_updated_checkpoint = checkpoint;
                cv.notify_all();
            }

            // Step four
            // TODO There is an issue where checkpoints aren't updated for next iteration like this
            // Rewrite update logic to include a next checkpoint value to use here
        } while (!should_exit());

        cout << "Thread Exit" << endl;
        delete done_to_checkpoint;
        delete prime;
    }

private:
    queue<uint64_t> *done_to_checkpoint;
    queue<uint64_t> *prime;

    // Keeps track of if the checkpoint has been updated yet
    uint64_t last_updated_checkpoint = 0;

    /**
     * Processes num, adding info to globals.result_table
     * num should be odd
     * start_num and checkpoint can be odd or even
     */
    void process(uint64_t num, uint64_t start_num, uint64_t checkpoint) {
        // During the loop we increment by 2 * num to hit only odd multiples of num
        uint64_t interval = 2 * num;

        // We calculate the start to be the first odd multiple of num larger than start_num
        uint64_t multiple = start_num % num + 1;
        multiple += 1 - (multiple % 2); // Adjust to be next odd number

        // We don't want to process num ever
        if (multiple == 1) {
            multiple += 2;
        }

        cout << "Proc: " << num << " start: " << multiple * num << " check: " << checkpoint << " interval: " << interval << endl << "   ";

        // Loop over every odd multiple of num between start_num and checkpoint
        for (uint64_t i = multiple * num; i < checkpoint; i = i + interval) {
            (*globals.result_table)[i] = false;
            cout << " " << i;
        }
        cout << endl;
    }

    /**
     * Drains prime, processing all elements in it
     */
    void drain_prime(uint64_t start_num, uint64_t checkpoint) {
        // We replace prime with a new queue because it is easy to just requeue into a new queue
        queue<uint64_t> *new_prime = new queue<uint64_t>();

        while (!prime->empty()) {
            uint64_t next = prime->front();
            process(next, start_num, checkpoint);
            new_prime->push(next);
            prime->pop();
        }

        delete prime;
        prime = new_prime;
    }

    /**
     * Drains done_to_checkpoint
     * Like drain_prime, but elements that aren't prime aren't requeued or processed
     */
    void drain_done_to_checkpoint(uint64_t start_num, uint64_t checkpoint) {
        while (!done_to_checkpoint->empty()) {
            uint64_t next = done_to_checkpoint->front();

            if (globals.is_prime(next)) {
                process(next, start_num, checkpoint);
                prime->push(next);
            }

            done_to_checkpoint->pop();
        }
    }

    /**
     * Returns true if the thread should exit because the program is done executing
     * In practice this returns true if the last checkpoint processed up to max_range
     */
    bool should_exit() {
        cout << "Exit? " << globals.checkpoint[0] << " " << globals.max_range << endl;
        return globals.checkpoint[0] >= globals.max_range;
    }

    /**
     * Processes the thread queue
     * This is the complicated part of the thread that must work well with the main thread to not deadlock or get stuck
     */
    void process_queue(uint64_t start_num, uint64_t checkpoint) {
        bool done = false;
        cout << "Thread Process Queue: " << start_num << " " << checkpoint << endl;
        while (!done) {
            while(!is_queue_empty()) {
                uint64_t next = q.front();
                q.pop();
                process(next, start_num, checkpoint);

                // Add to done queue for next checkpoint
                // TODO
            }

            // Check if the main thread is done queueing
            done = done_queueing();
            if (!done) {
                // Wait to be notified
                unique_lock<mutex> lk(mtx);
                cv.wait(lk);
            }
        }
    }

    /**
     * Checks if the thread queue is empty
     */
    bool is_queue_empty() {
        unique_lock<mutex> lk(mtx);
        return q.empty();
    }

    /**
     * Checks if the main thread is done queueing
     */
    bool done_queueing() {
        unique_lock<mutex> lk(globals.mtx);
        return globals.last_queued == globals.checkpoint[0];
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
    cout << "Main Waiting..." << endl;
    for (size_t i = 0; i < NUM_THREADS; i++) {
        unique_lock<mutex> lk(contexts[i].mtx);
        if (!contexts[i].done_checkpoint) {
            cout << "Wait t: " << i << endl;
            // If this thread isn't done, wait for it to finish
            contexts[i].cv.wait(lk);
        }
    }
}

/**
 * Notify all threads to continue work
 */
void notify_all_threads(ThreadContext *contexts) {
    for (size_t i = 0; i < NUM_THREADS; i++) {
        unique_lock<mutex> lk(contexts[i].mtx);
        contexts[i].cv.notify_all();
    }
}


int main() {
    uint64_t last_queued;

    globals.start_range = 2;
    globals.max_range = 40;
    // The max try is num/3 because the lowest multiple we might process is num * 3. Add 1 because range not inclusive.
    globals.max_try = globals.max_range / 3 + 1;

    // Set up checkpoints
    // The initial condition is all the same, they will be updated before every iteration
    globals.checkpoint[3] = globals.start_range;
    globals.checkpoint[2] = globals.start_range;
    globals.checkpoint[1] = globals.start_range;
    globals.checkpoint[0] = globals.start_range;

    globals.last_queued = globals.checkpoint[0];

    globals.result_table = NULL;
    globals.primes_table = NULL;
    const tbb::concurrent_unordered_map<uint64_t,bool> *results = NULL; // Results created by the threads

    // Split into threads
    thread threads[NUM_THREADS];
    ThreadContext contexts[NUM_THREADS]; // Relying on default initialization
    // Whether the threads have been started yet
    bool started = false;

    while (globals.checkpoint[0] < globals.max_range) {

        // All Threads are done by here
        {
            unique_lock<mutex> lk(globals.mtx);

            // We process this table later in this iteration
            results = globals.primes_table;
            globals.primes_table = globals.result_table;
            globals.result_table = new tbb::concurrent_unordered_map<uint64_t,bool>();

            globals.update_checkpoint();
            globals.cv.notify_all();
        }
        if (!started) {
            // Only start the threads running the first iteration
            for (size_t i = 0; i < NUM_THREADS; i++) {
                threads[i] = thread(&ThreadContext::run, &contexts[i]);
            }
            started = true;
        }

        size_t next_thread = 0;
        uint64_t checkpoint_try = globals.checkpoint[0] / 3 + 1;
        for (
                // Start at first odd number after end of last checkpoint
                uint64_t i = globals.checkpoint[1] + ((globals.checkpoint[1] + 1) % 2);
                i < checkpoint_try; // Iterate over every number less than sqrt(checkpoint)
                i = i + 2 // Iterate over odd numbers
                ) {
            cout << "Queue: " << i << " thread: " << next_thread << endl;
            next_thread = (next_thread + 1) % NUM_THREADS; // We do this first so it happens outside of the lock
            unique_lock<mutex> lk(contexts[next_thread].mtx);
            contexts[next_thread].q.push(i);
            contexts[next_thread].done_checkpoint = false;
            contexts[next_thread].cv.notify_all();
        }

        {
            unique_lock<mutex> lk(globals.mtx);
            globals.last_queued = globals.checkpoint[0];
            cout << "All Queued" << endl;

            // Here we must notify all threads again in case they checked last_queued to be false between when their
            // last value was finished and now correcting that race condition.
            // Note that we call this while still holding globals.mtx and notify_all_threads acquires individual
            // thread locks. This prevents threads from checking for a queue and then sleeping between when we finish
            // and when we declare we are done in a thread ordering where they see last_queued == last checkpoint but
            // do not sleep until after notify_all_threads is done.
            notify_all_threads(contexts);
        }

        // Results will be NULL on the first two loops
        if (results != NULL) {
            // Flush results to stdout so we can free up some memory
            // We iterate over the second-last to third-last checkpoints
            flush_primes(results, globals.checkpoint[3], globals.checkpoint[2]);
            delete results;
            results = NULL;
        }

        // Skip this step on the last iteration because threads will exit instead of wait
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
