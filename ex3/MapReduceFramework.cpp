#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "pthread.h"
#include <iostream>
#include <algorithm>
#include <atomic>
#include "Barrier.h"
#include "Barrier.cpp"

#define MAIN_THREAD 0
#define PTHREAD_CREATE_ERROR_MSG "system error: pthread_create failed"
#define PTHREAD_JOIN_ERROR_MSG "system error: pthread_join failed"
#define MUTEX_LOCK_ERROR_MSG "system error: pthread_lock failed"
#define MUTEX_UNLOCK_ERROR_MSG "system error: pthread_unlock failed"


static uint64_t take_counter = (uint64_t) 1 << 31;

typedef std::vector<std::pair<pthread_t, IntermediateVec *>> Inter_vec;
typedef std::vector<std::pair<K2 *, IntermediateVec *>> Shuffled_vec;

int comp(IntermediatePair p1, IntermediatePair p2) {
    return (*p1.first < *p2.first);
}

void mutex_lock(pthread_mutex_t *mutex) {
    if (pthread_mutex_lock(mutex) != 0) {
        std::cout << MUTEX_LOCK_ERROR_MSG << std::endl;
        exit(1);
    }
}

void mutex_unlock(pthread_mutex_t *mutex) {
    if (pthread_mutex_unlock(mutex) != 0) {
        std::cout << MUTEX_UNLOCK_ERROR_MSG << std::endl;
        exit(1);
    }
}

struct jobHandler {
    Inter_vec *inter_vec{};
    std::atomic<int64_t> atomic_counter{};
    OutputVec *outputVec{};
    Shuffled_vec *shuffled_vec{};
    const InputVec *inputVec{};
    const MapReduceClient *client{};
    pthread_t *shuffle_thread{};
    Barrier *barrier{};
    pthread_t *threads_arr{};
    JobState *jobState = new JobState();
    int threads_num{};
    bool is_wait = false;
    int shuffle_size = 0;
    int reduce_size = 0;
    bool map_pass = true;
    bool reduce_pass = true;

    //-----mutexes-----
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_state = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_sort_pass = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_map = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_map_pass = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_reduce_pass = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_reduce = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_emit2 = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_emit3 = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_wait = PTHREAD_MUTEX_INITIALIZER;
    //-----mutexes-----

};

/**
 * when passing from stage to stage. change the stage to the new stage and
 * update the total to the relevant total according to the stage. the 2 msb
 * of atomic_counter are the stage, the next 31 bits are the total and the
 * 31 lsb bits are the counter
 * @param job
 * @param new_total
 */
void change_stage_and_total(JobHandle job, size_t new_total) {
    auto *job_handler = static_cast<jobHandler *>(job);
    unsigned long total = new_total;
    total = total << 31;
    unsigned long stage = job_handler->jobState->stage;
    stage = stage << 62;
    unsigned long total_and_stage = stage + total;
    job_handler->atomic_counter = (long long) total_and_stage;
}

/**
 * update the stage and the percentage to the new stage and percentage
 * @param job
 * @param state
 */
void getJobState(JobHandle job, JobState *state) {
    auto *job_handler = static_cast<jobHandler *>(job);
    mutex_lock(&job_handler->mutex_state);
    unsigned long atomic_counter = job_handler->atomic_counter.load();
    unsigned long counter = (atomic_counter % take_counter);
    unsigned long total = (atomic_counter << 2) >> 33;
    unsigned long stage = atomic_counter >> 62;
    state->stage = (stage_t) stage;
    if (stage == 0) {
        state->percentage = 0;
    } else {
        state->percentage = (float) (((float) counter / (float) total) * 100.0);
    }
    mutex_unlock(&job_handler->mutex_state);
}

/**
 * add the already processed pair of key and value to the inter_vec
 * @param key
 * @param value
 * @param context
 */
void emit2(K2 *key, V2 *value, void *context) {
    auto job_handler = static_cast<jobHandler *>(context);
    mutex_lock(&job_handler->mutex_emit2);
    for (auto pair: *(job_handler->inter_vec)) {
        if (pair.first == pthread_self()) {
            pair.second->push_back(std::make_pair(key, value));
            job_handler->shuffle_size++;
        }
    }
    mutex_unlock(&job_handler->mutex_emit2);
}

/**
 * add the already processed pair of key and value to the output_vec
 * @param key
 * @param value
 * @param context
 */
void emit3(K3 *key, V3 *value, void *context) {
    auto *job_handler = static_cast<jobHandler *>(context);
    mutex_lock(&job_handler->mutex_emit3);
    job_handler->outputVec->push_back(std::make_pair(key, value));
    mutex_unlock(&job_handler->mutex_emit3);
}

/**
 * runs user map function on all of the input vector
 * @param job_handler
 */
void map(jobHandler *job_handler) {
    while ((job_handler->atomic_counter % take_counter) <
           job_handler->inputVec->size()) {
        mutex_lock(&job_handler->mutex_map);
        if ((job_handler->atomic_counter % take_counter) <
            job_handler->inputVec->size()) {
            job_handler->client->map(
                    (*(job_handler->inputVec))[job_handler->atomic_counter
                                               % take_counter].first,
                    (*(job_handler->inputVec))[job_handler->
                            atomic_counter % take_counter].second,
                    job_handler);
            job_handler->atomic_counter++;
        }
        mutex_unlock(&job_handler->mutex_map);
    }
}

void sort(jobHandler *job_handler) {
    mutex_lock(&job_handler->mutex_sort_pass);
    for (auto pair: *(job_handler->inter_vec)) {
        if (pair.first == pthread_self()) {
            std::sort(pair.second->begin(), pair.second->end(), comp);
        }
    }
    mutex_unlock(&job_handler->mutex_sort_pass);
}

/**
 * shuffle the intermediate vector to get key and vector of values
 * @param job_handler
 */
void shuffle(jobHandler *job_handler) {
    change_stage_and_total(job_handler,
                           job_handler->shuffle_size);
    for (auto pthread_vec: *(job_handler->inter_vec)) {
        // pair<pthread_t, vec<pair<key*, value*>*>
        for (auto pair: *(pthread_vec.second)) {
            // pair<key*, value*> = <K2*, V2*>
            bool exist = false;
            for (auto k2_to_vec_pair: *job_handler->shuffled_vec) {
                if (!(*k2_to_vec_pair.first < *pair.first) &&
                    !(*pair.first < *k2_to_vec_pair.first)) {
                    exist = true;
                    k2_to_vec_pair.second->push_back(pair);
                    job_handler->atomic_counter++;
                    continue;
                }
            }
            if (!exist) {
                auto new_vec = new IntermediateVec();
                new_vec->push_back(pair);
                (*job_handler->shuffled_vec).push_back(
                        std::make_pair(pair.first, new_vec));
                job_handler->atomic_counter++;
            }
        }
    }
}

/**
 * runs the user reduce function on the intermediate vec to get the output
 * vector
 * @param job_handler
 */
void reduce(jobHandler *job_handler) {
    while ((job_handler->atomic_counter % take_counter) < (job_handler->shuffled_vec->size())) {
        mutex_lock(&job_handler->mutex_reduce);
        // need to add the if statement because the threads are waiting after
        // the while condition and may enter all together to the loop and
        // cause SIGSEGV
        if ((job_handler->atomic_counter % take_counter) < (job_handler->shuffled_vec->size())) {
            job_handler->client->reduce(
                    (job_handler->shuffled_vec->begin() + (int) (job_handler->atomic_counter % take_counter))->second,
                    job_handler);
            job_handler->atomic_counter++;
        }
        mutex_unlock(&job_handler->mutex_reduce);
    }
}

jobHandler *
init_handler(const MapReduceClient &client, const InputVec &inputVec,
             OutputVec &outputVec, int multiThreadLevel) {
    auto *job_handler = new jobHandler();
    job_handler->barrier = new Barrier(multiThreadLevel);
    job_handler->inter_vec = new Inter_vec;
    job_handler->threads_arr = new pthread_t[multiThreadLevel];
    job_handler->shuffled_vec = new Shuffled_vec;
    job_handler->inputVec = &inputVec;
    job_handler->outputVec = &outputVec;
    job_handler->client = &client;
    job_handler->atomic_counter = 0;
    job_handler->jobState->percentage = 0.0;
    job_handler->threads_num = multiThreadLevel;
    return job_handler;
}

/**
 * main function that wraps the user map and reduce. runs user map function on
 * the input, then shuffle all the data, and then runs user reduce function.
 * @param arg
 * @return
 */
void *MapReduce(void *arg) {
    auto *job_handler = static_cast<jobHandler *>(arg);

    mutex_lock(&job_handler->mutex_emit2);
    job_handler->inter_vec->push_back(std::make_pair
                                              (pthread_self(),
                                               new IntermediateVec()));
    mutex_unlock(&job_handler->mutex_emit2);


    // ----------- map and sort phase -----------
    mutex_lock(&job_handler->mutex_map_pass);
    if (job_handler->map_pass) {
        job_handler->map_pass = false;
        job_handler->jobState->stage = MAP_STAGE;
        change_stage_and_total(job_handler,
                               job_handler->inputVec->size());
    }
    mutex_unlock(&job_handler->mutex_map_pass);

    map(job_handler);

    job_handler->barrier->barrier();

    sort(job_handler);

    job_handler->barrier->barrier();

    // ----------- shuffle phase ------------

    if (pthread_self() == *job_handler->shuffle_thread) {
        job_handler->jobState->stage = SHUFFLE_STAGE;
        shuffle(job_handler);
    }

    job_handler->barrier->barrier();

    // ----------- reduce phase ------------

    mutex_lock(&job_handler->mutex_reduce_pass);
    if (job_handler->reduce_pass) {
        job_handler->reduce_pass = false;
        job_handler->reduce_size = (int) job_handler->shuffled_vec->size();
        job_handler->jobState->stage = REDUCE_STAGE;
        change_stage_and_total(job_handler,
                               job_handler->reduce_size);
    }
    mutex_unlock(&job_handler->mutex_reduce_pass);

    reduce(job_handler);

    return job_handler;
}

/**
 * create the threads and initiate the counter
 * @param client pointer to the class that responsible for custom map and
 * reduce functions
 * @param inputVec input vector of all items to operate map reduce on
 * @param outputVec vector of all items after the operation of map reduce
 * @param multiThreadLevel number of threads in the process
 * @return pointer to the JobHandle
 */
JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec,
                  OutputVec &outputVec, int multiThreadLevel) {

    jobHandler *job_handler = init_handler(client, inputVec, outputVec,
                                           multiThreadLevel);
    for (int thread_ind = 0; thread_ind < multiThreadLevel; thread_ind++) {
        if (pthread_create(job_handler->threads_arr + thread_ind,
                           nullptr, MapReduce, job_handler)
            != 0) {
            std::cout << PTHREAD_CREATE_ERROR_MSG << std::endl;
            exit(1);
        }
        if (thread_ind == MAIN_THREAD) {
            job_handler->shuffle_thread =
                    job_handler->threads_arr + thread_ind;
        }
    }
    return job_handler;
}

/**jobHandler
 *
 * @param job job to wait
 */
void waitForJob(JobHandle job) {
    auto job_handler = static_cast<jobHandler *>(job);
    mutex_lock(&job_handler->mutex_wait);
    if (!job_handler->is_wait) {
        for (int thread_ind = 0;
             thread_ind < job_handler->threads_num; thread_ind++) {
            if (pthread_join(job_handler->threads_arr[thread_ind],
                             nullptr) != 0) {
                std::cout << PTHREAD_JOIN_ERROR_MSG << std::endl;
                exit(1);
            }
        }
        job_handler->is_wait = true;
    }
    mutex_unlock(&job_handler->mutex_wait);
}

void closeJobHandle(JobHandle job) {
    waitForJob(job);
    auto *job_handler = static_cast<jobHandler *>( job);
    pthread_mutex_destroy(&job_handler->mutex);
    pthread_mutex_destroy(&job_handler->mutex_emit2);
    pthread_mutex_destroy(&job_handler->mutex_emit3);
    pthread_mutex_destroy(&job_handler->mutex_map);
    pthread_mutex_destroy(&job_handler->mutex_reduce);
    pthread_mutex_destroy(&job_handler->mutex_reduce_pass);
    pthread_mutex_destroy(&job_handler->mutex_state);
    pthread_mutex_destroy(&job_handler->mutex_wait);
    for (auto pair: *(job_handler->inter_vec)) {
        delete pair.second;
    }
    for(size_t i = 0; i < job_handler->shuffled_vec->size(); ++i) {
        delete (job_handler->shuffled_vec->begin() + (int)i)->second;
    }
//    } (!(job_handler->shuffled_vec->empty()) {
//        delete job_handler->shuffled_vec->begin()->second;
//        job_handler->shuffled_vec->erase(job_handler->shuffled_vec->begin());
//    }
    delete job_handler->inter_vec;
    delete job_handler->shuffled_vec;
    delete[] job_handler->threads_arr;
    delete job_handler->barrier;
    delete job_handler->jobState;
    delete job_handler;
}