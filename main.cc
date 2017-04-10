#include <iostream>
#include <math.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>

#include <tbb/concurrent_unordered_map.h>

using namespace std;

uint64_t max_range = 100;
uint64_t start_range = 3;

// 2 * because for large numbers we could experience a floating point error, but of less than a factor of max
uint64_t max_try = 2 * uint64_t(sqrtl(max_range));

// Tuning variables
size_t NUM_THREADS = 7;


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

    bool exit_when_finished;

    ThreadContext() : exit_when_finished(false) {}
};


/**
 * Contains info used by all threads
 * We use a concurrent version of std::unordered_map to avoid complicated locking
 */
class GlobalInfo {
public:
    tbb::concurrent_unordered_map<uint64_t,bool> result_table;
};
GlobalInfo globals;


/**
 * Processes num, adding info to globals.result_table
 */
void process(uint64_t num) {
    uint64_t interval = 2 * num;

    // Start never goes below 0 or funky stuff happens
    // Always start at least at 2 * num
    uint64_t start = (start_range / interval) * interval + interval;
    if (start % 2 == 0) {
        start += num;
    }

    for (uint64_t i = start; i < max_range; i = i + interval) {
        globals.result_table[i] = false;
    }
}


/**
 * The main thread loop for this program.
 * info is the local ThreadContext object for this thread.
 *
 * The loop essentially does three things over and over until the main thread exits and is finished.
 */
void run(ThreadContext *info) {
    while(true) {
        uint64_t next;

        {
            /**
             * Just a block to hold our lock ~music notes emoji~
             */
            unique_lock<mutex> lk(info->mtx);

            while(info->q.empty() && !info->exit_when_finished) {
                info->cv.wait(lk);
            }

            if (info->q.empty() && info->exit_when_finished) {
                break;
            }

            next = info->q.front();
            info->q.pop();
        }

        process(next);
    }
}


int main() {
    uint64_t last_queued;

    // Split into threads
    vector<thread> threads(NUM_THREADS);
    vector<ThreadContext> contexts(NUM_THREADS);
    for (size_t i = 0; i < NUM_THREADS; i++) {
        threads[i] = thread(run, &contexts[i]);
    }

    size_t next_thread = 0;
    for (uint64_t i = 3; i < max_try; i = i + 2) {
        unique_lock<mutex> lk(contexts[next_thread].mtx);
        contexts[next_thread].q.push(i);
        contexts[next_thread].exit_when_finished = true;
        contexts[next_thread].cv.notify_all();
        lk.unlock();
        next_thread = (next_thread + 1) % NUM_THREADS;
    }

    for (size_t i = 0; i < NUM_THREADS; i++) {
        threads[i].join();
    }

    // Go through map and pull out results
    for (uint64_t i = start_range; i < max_range; i = i + 2) {
        auto it = globals.result_table.find(i);
        if (it == globals.result_table.end() || it->second) {
            cout << i << endl;
        }
    }
}

/**
 * Stretch TODOs
 * Add splitting of processing by batches
 *      For large batches:
 *          Process some amount as batch
 *          Reduce memory to only the primes / write primes to disk and dump memory
 *          Start second batch for second set of numbers
 *              Later batches only have to already found primes from batches smaller than them
 *
 * If it can be guaranteed at any point that all future numbers will be larger than a certain number in the map
 * Remove the number form the map and add it to results so far list
 * Maybe this could be done with a block-processing system?
 *      For item removed from queue:
 *          process up to next queue checkpoint, then enqueue
 *          If removed from queue before checkpoint updated, then wait or something (Or separate queue for checkpointed?)
 *          Allow items to be removed from map when checkpoint updated
 */
