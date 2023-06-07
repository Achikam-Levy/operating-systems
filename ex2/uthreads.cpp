#include <iostream>
#include <map>
#include <deque>
#include "uthreads.h"
#include "Thread.h"
#include <csignal>
#include <sys/time.h>
#include <set>
#include <vector>
#include <queue>
#include <csetjmp>
#include <cstdlib>

#define MILLION 1000000
#define MAIN_THREAD 0
#define MIN_THREAD_ID 0
#define FAILED -1
#define TERMINATED_ID -1
#define BLOCK_MAIN_THREAD_ERROR_MESSAGE "thread library error: you should not block the main thread"
#define MAIN_THREAD_SLEEP_ERROR_MESSAGE "thread library error: main thread shouldn't call this function"
#define INVALID_USECS_ERROR_MESSAGE "thread library error: non-positive input value"
#define TOO_MANY_THREADS_ERROR_MESSAGE "thread library error: too many threads created"
#define NULL_ENTRY_POINT_ERROR_MESSAGE "thread library error: entry point should not be null"
#define NO_THREAD_ERROR_MESSAGE "thread library error: there is not such thread"
#define MASKING_FAIL_ERROR_MESSAGE "system error: masking failed"
#define INVALID_TID_ERROR_MESSAGE "tid number should be between 0 to 99"
#define SIG_ACTION_FAILED_ERROR_MESSAGE "system error: sigaction fail"
#define SET_TIMER_FAILED_ERROR_MESSAGE "system error: setitimer fail"



using std::vector;
using std::deque;
using std::cout;
using std::endl;

std::map<int, Thread *> threads_map;
std::map<int, vector<int>> sleep_map;
std::set<int> blocked_set;
std::set<int> sleep_set;
std::deque<int> ready_deq;
std::priority_queue<int, std::vector<int>, std::greater<int>> available_id_queue;
std::vector<int> unavailable_id_vec;

int running_thread_id;
int threads_num = 0;
int total_quantum_counter = 0;
struct sigaction sa = {nullptr};
struct itimerval timer;
sigset_t maskedSignals; // mask signals


int init_timer (int quantum_usecs);

std::vector<int>::iterator find_value_in_vec (int tid);
std::_Deque_iterator<int, int &, int *> find_value_in_deque (int tid);


void wake_from_sleep();

void initialize_available_id_queue();

int start_new_timer();

void terminate_main_thread();

void delete_thread(int tid);

void move_to_next_thread();

int mask(int state);

/**
 * switches the running thread to the next thread, which is the first thread
 * in the ready_deq. it called every time the timer finish or when
 * terminating \ blocking \ sleeping the running thread.
 * @param sig signal from the operation system, unused by us.
 */
void switch_threads (int sig)
{
    mask(SIG_BLOCK);
    total_quantum_counter++;
    wake_from_sleep();

    // if the timer finish normally or the running thread was blocked or got
    // slept (not when terminated the running thread)
    if (running_thread_id != TERMINATED_ID)
    {
        // save the current state of the running thread
        int just_saved_state = sigsetjmp
        (threads_map[running_thread_id]->get_env(), 1);
        if (just_saved_state != 0) // we got here from siglongjmp and we don't
                                    // want to change the running thread
        {
            mask(SIG_UNBLOCK);
            return;
        }
        if(!ready_deq.empty()){
            move_to_next_thread();
        } else{
            threads_map[running_thread_id]->increase_running_quantums();
            mask(SIG_UNBLOCK);
            return;
        }
    }
    // in case that we got here after terminates the running thread
    else
    {
        if (!ready_deq.empty())
        {
            move_to_next_thread();
        }
    }
}

/**
 * load the next thread to run and moves to it with siglongjmp.
 * signals already masked before calling to this function, so it has to
 * unmask them.
 */
void move_to_next_thread()
{
    // if we didn't get here from block / terminate we want to add the
    // running thread to the ready_deq
    if((running_thread_id != TERMINATED_ID && threads_map[running_thread_id]->get_state
            () != BLOCKED) && sleep_set.count(running_thread_id) == MAIN_THREAD)
        {
        threads_map[running_thread_id]->set_state(READY);
        ready_deq.push_back(running_thread_id);
        }
    running_thread_id = ready_deq.front();
    ready_deq.pop_front();
    threads_map[running_thread_id]->set_state(RUNNING);
    threads_map[running_thread_id]->increase_running_quantums();
    siglongjmp(threads_map[running_thread_id]->get_env(), 1);
}

/**
 * wake all the threads that their sleeping time passed. if a thread is also
 * blocked, it remains blocked
 */
void wake_from_sleep()
{
    for (int id : sleep_map[total_quantum_counter])
    {
        if(threads_map[id] != nullptr){
            sleep_set.erase(id);
            if(blocked_set.count(id) == 0){
                ready_deq.push_back (id);
            }
        }
    }
    if(sleep_map.count(total_quantum_counter) > 0){
        sleep_map.erase(total_quantum_counter);
    }
}

/**
 * init the ready queue with all the available id's, the signals to mask,
 * the timer and the main thread. Then start execute the main thread (tid 0)
 * @param quantum_usecs between context switches in microseconds
 * @return 0 upon success, -1 upon failure
 */
int uthread_init (int quantum_usecs)
{
    if (quantum_usecs <= 0)
    {
        std::cerr << INVALID_USECS_ERROR_MESSAGE<<endl;
        return FAILED;
    }
    sigemptyset(&maskedSignals);
    sigaddset(&maskedSignals, SIGVTALRM);

    initialize_available_id_queue();

    auto* new_thread = new Thread (available_id_queue.top(), RUNNING,nullptr);
    threads_map[available_id_queue.top()] = new_thread;
    unavailable_id_vec.push_back (available_id_queue.top());
    running_thread_id = available_id_queue.top();
    available_id_queue.pop();
    threads_num++;
    new_thread->setup_thread (new_thread->get_id (), new_thread->get_stack (),
                              new_thread->get_entry_point ());
    init_timer(quantum_usecs);
    if(start_new_timer()== FAILED){
        return FAILED;
    }
    switch_threads(0);
    return EXIT_SUCCESS;
}


void initialize_available_id_queue()
{
    for (int id = 0; id < MAX_THREAD_NUM; id++)
    {
        available_id_queue.push (id);
    }
}

/**
 * initialize the timer and the timer handler, which will be called every
 * time the timer complete interval
 * @param quantum_usecs between context switches in microseconds
 * @return 0 upon success, -1 upon failure
 */
int init_timer (int quantum_usecs)
{
    timer.it_value.tv_sec = quantum_usecs / MILLION;
    timer.it_value.tv_usec = quantum_usecs % MILLION;

    timer.it_interval.tv_sec = quantum_usecs / MILLION;
    timer.it_interval.tv_usec = quantum_usecs % MILLION;

    // Install timer_handler as the signal handler for SIGVTALRM.
    sa.sa_handler = &switch_threads;
    if (sigaction (SIGVTALRM, &sa, nullptr) == FAILED)
    {
        std::cerr << SIG_ACTION_FAILED_ERROR_MESSAGE << endl;
        return FAILED;
    }
    return EXIT_SUCCESS;
}

/**
 * start new timer
 */
int start_new_timer()
{
    if (setitimer (ITIMER_VIRTUAL, &timer, nullptr) == FAILED)
    {
        std::cerr << SET_TIMER_FAILED_ERROR_MESSAGE<<endl;
        return FAILED;
    }
    return EXIT_SUCCESS;
}


int uthread_spawn (thread_entry_point entry_point)
{
    mask(SIG_BLOCK);
    if ((threads_num + 1) > MAX_THREAD_NUM)
    {
        std::cerr << TOO_MANY_THREADS_ERROR_MESSAGE<<endl;
        mask(SIG_UNBLOCK);
        return FAILED;
    }
    if (!entry_point)
    {
        std::cerr << NULL_ENTRY_POINT_ERROR_MESSAGE<<endl;
        mask(SIG_UNBLOCK);
        return FAILED;
    }
    int current_id = available_id_queue.top ();
    auto *new_thread = new Thread (current_id, READY, entry_point);
    threads_map[current_id] = new_thread;
    unavailable_id_vec.push_back (current_id);
    available_id_queue.pop ();
    ready_deq.push_back (new_thread->get_id ());
    threads_num++;
    new_thread->setup_thread (new_thread->get_id (), new_thread->get_stack (),
                              new_thread->get_entry_point ());
    mask(SIG_UNBLOCK);
    return new_thread->get_id ();
}

int uthread_terminate (int tid)
{
    mask(SIG_BLOCK);
    // if invalid thread tid
    if (tid < MIN_THREAD_ID || tid > (MAX_THREAD_NUM - 1))
    {
        std::cerr << INVALID_TID_ERROR_MESSAGE << endl;
        mask(SIG_UNBLOCK);
        return FAILED;
    }
    // if thread not exist
    if (threads_map.count (tid) == 0)
    {
        std::cerr << NO_THREAD_ERROR_MESSAGE<<endl;
        mask(SIG_UNBLOCK);
        return FAILED;
    }
    // if try to terminate the main thread
    if (tid == 0)
    {
        terminate_main_thread();
        mask(SIG_UNBLOCK);
        exit (0);
    }
//   if try to terminate thread that already blocked
    if (blocked_set.count (tid) > 0)
    {
        blocked_set.erase (tid);
    }
    delete_thread(tid);
    if(running_thread_id == tid){
        running_thread_id = TERMINATED_ID;
        if(start_new_timer()== FAILED){
            mask(SIG_UNBLOCK);
            return FAILED;
        }
        mask(SIG_UNBLOCK);
        switch_threads(0);
    }
    mask(SIG_UNBLOCK);
    return 0;
}

/**
 * block or unblock signals
 * @param state 1 to block signals, 0 to unblock
 * @return 0 upon success
 */
int mask(int state)
{
    if(sigprocmask(state, &maskedSignals, nullptr) == FAILED)
    {
        std::cerr <<MASKING_FAIL_ERROR_MESSAGE<< std::endl;
        terminate_main_thread();
        exit(0);
    }
    return 0;
}

/**
 * delete thread with id tid, and release all the resources allocated to it
 * @param tid
 */
void delete_thread(int tid)
{
    delete threads_map[tid];
    threads_map.erase (tid);
    available_id_queue.push (tid);
    auto delete_from_vec = find_value_in_vec (tid);
    if(delete_from_vec!= unavailable_id_vec.end()){
        unavailable_id_vec.erase (delete_from_vec);
    }
    if(sleep_map.count(tid) > 0){
        sleep_map.erase(tid);
    }
    if(blocked_set.count(tid) > 0){
        blocked_set.erase(tid);
    }
    auto delete_from_deq = find_value_in_deque (tid);
    if(delete_from_deq != ready_deq.end()){
        ready_deq.erase (delete_from_deq);
    }
    threads_num--;
}

/**
 * release all the resources of all of the threads
 */
void terminate_main_thread()
{
    for (auto &thread: threads_map)
    {
        delete thread.second;
    }
    threads_map.clear();
}

int uthread_block (int tid)
{
    mask(SIG_BLOCK);
    // if invalid tid number
    if (tid < MIN_THREAD_ID || tid > (MAX_THREAD_NUM - 1))
    {
        std::cerr << INVALID_TID_ERROR_MESSAGE << endl;
        mask(SIG_UNBLOCK);
        return FAILED;
    }
    // if threads not exist
    if (threads_map.count (tid) == 0){
        std::cerr << NO_THREAD_ERROR_MESSAGE<<endl;
        mask(SIG_UNBLOCK);
        return FAILED;
    }
    // if try to block the main thread
    if (tid == 0){
        std::cerr<< BLOCK_MAIN_THREAD_ERROR_MESSAGE<<endl;
        mask(SIG_UNBLOCK);
        return FAILED;
    }
    // if try to block a thread that already blocked
    if (blocked_set.count(tid) > 0){
        mask(SIG_UNBLOCK);
        return EXIT_SUCCESS;
    }
    threads_map[tid]->set_state (BLOCKED);
    blocked_set.insert (tid);
    // if blocks the running thread
    if (running_thread_id == tid){
        if(start_new_timer()== FAILED){
            mask(SIG_UNBLOCK);
            return FAILED;
        }
        mask(SIG_UNBLOCK);
        switch_threads (0);
        return EXIT_SUCCESS;
    }
    // if blocks thread from the ready_deq, i.e. not the running thread or
    // blocks the running thread
    std::_Deque_iterator<int, int &, int *> id_to_delete = find_value_in_deque (tid);
    if(id_to_delete != ready_deq.end()){
        ready_deq.erase (id_to_delete);
    }
    mask(SIG_UNBLOCK);
    return EXIT_SUCCESS;
}

int uthread_resume (int tid)
{
    mask(SIG_BLOCK);
    // if invalid tid number
    if (tid < MIN_THREAD_ID || tid > (MAX_THREAD_NUM - 1))
    {
        std::cerr << INVALID_TID_ERROR_MESSAGE << endl;
        mask(SIG_UNBLOCK);
        return FAILED;
    }
    if (threads_map.count (tid) == 0)
    {
        std::cerr << NO_THREAD_ERROR_MESSAGE<<endl;
        mask(SIG_UNBLOCK);
        return FAILED;
    }
    if (blocked_set.count(tid) > 0)
    {
        threads_map[tid]->set_state (READY);
        blocked_set.erase (tid);
        if(sleep_set.count(tid) == 0){
            ready_deq.push_back (tid);
        }
    }
    mask(SIG_UNBLOCK);
    return EXIT_SUCCESS;
}

int uthread_sleep (int num_quantums)
{
    mask(SIG_BLOCK);
    // this function can be used only on the running thread
    int current_thread_id = running_thread_id;
    // if sleeps the main thread
    if (current_thread_id == MAIN_THREAD)
    {
        std::cerr << MAIN_THREAD_SLEEP_ERROR_MESSAGE<<endl;
        mask(SIG_UNBLOCK);
        return FAILED;
    }
    sleep_map[total_quantum_counter + num_quantums + 1].push_back
            (current_thread_id);
    threads_map[current_thread_id]->set_state(READY);
    if(start_new_timer()== FAILED){
        mask(SIG_UNBLOCK);
        return FAILED;
    }
    sleep_set.insert(running_thread_id);
    mask(SIG_UNBLOCK);
    switch_threads (num_quantums);
    return EXIT_SUCCESS;
}

int uthread_get_tid ()
{
    return running_thread_id;
}

int uthread_get_total_quantums ()
{
    return total_quantum_counter;
}

int uthread_get_quantums (int tid)
{
    // if invalid tid number
    if (tid < MIN_THREAD_ID || tid > (MAX_THREAD_NUM - 1))
    {
        std::cerr << INVALID_TID_ERROR_MESSAGE << endl;
        return FAILED;
    }
    if(threads_map.count(tid) == 0){
        std::cerr<<NO_THREAD_ERROR_MESSAGE<<endl;
        return FAILED;
    }
    return threads_map[tid]->get_running_quantums ();
}

std::_Deque_iterator<int, int &, int *> find_value_in_deque (int tid)
{
    deque<int>::iterator id_to_delete = ready_deq.end();
    for (auto it = ready_deq.begin (); it != ready_deq.end ();
         it++)
    {
        if (*it == tid)
        {
            id_to_delete = it;
        }
    }
    return id_to_delete;
}

vector<int>::iterator find_value_in_vec (int tid)
{
    std::vector<int>::iterator id_to_delete = unavailable_id_vec.end();
    for (auto it = unavailable_id_vec.begin (); it != unavailable_id_vec.end ();
         it++)
    {
        if (*it == tid)
        {
            id_to_delete = it;
        }
    }
    return id_to_delete;
}