/*
*@ =============================================================================================
*@      Project:            
*@      File:               thread_worker
*@      Author:             mdcow
*@      Date:               2017-03-22 10:21:11
*@      Last Modified by:   mdcow
*@      Last Modified time: 2017-03-26 09:00:47
*@ =============================================================================================
*/

#include <cstring>
#include <cassert>
#include <memory>

#include <thread>
#include <cstdint>
#include <iostream>
#include <ctime>

// #define _atomic_increment(x, y)            __sync_fetch_and_add(x, y)
// #define _atomic_decrement(x, y)            __sync_fetch_and_sub(x, y)
// clang++ thread_worker7.cpp -pthread -Wall -pedantic -std=c++14 -o thread_worker7 && ./thread_worker7
// clang++ thread_worker7.cpp -pthread -Wall -pedantic -std=c++14 -o thread_worker7

#define _atomic_increment(x)            __sync_fetch_and_add(x, 1)
#define _atomic_decrement(x)            __sync_fetch_and_sub(x, 1)

#define _atomic_compare_swap            __sync_val_compare_and_swap
#define _atomic_swap                    __sync_lock_test_and_set

#define _compiler_barrier()             __asm__ __volatile__(""      ::: "memory")
#define _yield()                        __asm__ __volatile__("pause" ::: "memory") // __mm__pause ?

;;
using int8                              = int8_t;
using int16                             = int16_t;
using int32                             = int32_t;
using int64                             = int64_t;
using uint8                             = uint8_t;
using uint16                            = uint16_t;
using uint32                            = uint32_t;
using uint64                            = uint64_t;
using Job                               = struct Job;
using JobFunction                       = void (*)(Job*, const void*);
using job_stealing_worker               = class job_stealing_worker;


struct constants;
struct thread_args;
struct Job;
class  job_stealing_worker;
class  scheduler;


// -- POSIX_THREADS --
#include <pthread.h>
#include <unistd.h>
#define THREAD_FUNCT void*
#define THREAD_LOCAL __thread

// const uint32 EVENTWAIT_INFINITE = -1;
typedef pthread_t threadid_t;
struct eventid_t
{
    pthread_cond_t  cond;
    pthread_mutex_t mutex;
};

inline 
bool thread_create(threadid_t* returnid, void* (*start_func)(void*), void* parg_) {
    int32  retval = pthread_create(returnid, nullptr, start_func, parg_);
    return retval == 0;
}

inline 
bool thread_terminate(threadid_t threadid) {
    return pthread_cancel(threadid) == 0;
}

inline 
uint16 get_no_logical_cores() {
    return (uint16)sysconf(_SC_NPROCESSORS_ONLN);
}

inline 
eventid_t event_create() {
    eventid_t event = { PTHREAD_COND_INITIALIZER, PTHREAD_MUTEX_INITIALIZER };
    return event;
}
// -- ~POSIX_THREADS --



struct Job 
{
    JobFunction function;    
    int32 unfinishedJobs;
    char data[52 + 4*16 + 4*16];    
    Job* parent;
    // int32 continuationCount;
    // Job* continuations[14];
};


// -- CONSTANTS --
struct constants
{
    static const uint16     g_max_no_jobs    = 64*64u;
    static const uint16     g_mask           = g_max_no_jobs-1u;
    static const uint16     g_max_no_workers = 11u;
};
// -- ~CONSTANTS --

// -- THREADWORKER --
class job_stealing_worker : public constants
{
private:     
    int32  m_top;
    int32  m_bottom;
    Job*   m_jobs[g_max_no_jobs];
  
    void push_job(Job* job) {
        int32 bottom = m_bottom;
        m_jobs[bottom & g_mask] = job;
        _compiler_barrier();
        m_bottom = bottom + 1;    
    }

    Job* pop_job() {
        int32 bottom = m_bottom - 1;
        m_bottom     = bottom;
        _atomic_swap(&m_bottom, bottom);
        int32 top    = m_top;
        if (top <= bottom) {
            Job* job = m_jobs[bottom & g_mask];
            if (top != bottom) { return job; }
            if (_atomic_compare_swap(&m_top, top + 1, top) != top) {
                job  = nullptr;
            }
            m_bottom = top + 1;
            return job;
        } else {
            m_bottom = top;
            return nullptr;
        }
    }

public:
    Job* steal_job() {
        int32 top    = m_top;
        _compiler_barrier();
        int32 bottom = m_bottom;        
        if (top < bottom) {
            Job* job = m_jobs[top & g_mask];
            if (_atomic_compare_swap(&m_top, top + 1, top) != top) {
                return nullptr;
            }
            return job;
        } else { return nullptr; }
    }

    job_stealing_worker()
    : m_top(0) 
    , m_bottom(0) 
    {
        memset(&m_jobs, 0, sizeof(m_jobs));
    }

    friend class scheduler;
};
// -- ~THREADWORKER --


#define THREAD_LOCAL_STATIC static THREAD_LOCAL
// -- UTILITY --
    THREAD_LOCAL_STATIC
    uint16 tls_thread_num;

    THREAD_LOCAL_STATIC 
    uint16 tls_worker_id;
// -- ~UTILITY --


struct scheduler_stack : public constants
{
    THREAD_LOCAL_STATIC
    uint32                  g_allocatedJobs;    
    THREAD_LOCAL_STATIC
    Job                     g_jobAllocator[g_max_no_jobs];
};

namespace
{
    uint16 get_cores() { return (uint16)sysconf(_SC_NPROCESSORS_ONLN); }
    job_stealing_worker*    g_jobQueues   [16u]; 
    job_stealing_worker*    get_worker() { return g_jobQueues[tls_worker_id]; }
}


// -- SCHEDULER --
class scheduler : public scheduler_stack
{
public:
 
    uint16                  m_cores_available;
    uint16                  m_active_threads;
    uint16                  m_running_threads;

    job_stealing_worker*    m_pworker_store;
    thread_args*            m_pthread_store;
    threadid_t*             m_pthread_IDs;
    
    bool                    m_is_running;
    
    static
    Job* fetch_job() {
        job_stealing_worker* queue = get_worker();
        Job* job = queue->pop_job();
        if (is_job_empty(job)) {      

            auto generate_random = [](int16 x, int16 y) {
                std::srand(std::time(nullptr));
                return std::rand() % y;
            };
            uint16 randomIndex = generate_random(0, g_max_no_workers + 1);
            job_stealing_worker* victimQueue = g_jobQueues[randomIndex];
            if (victimQueue == queue) {
                _yield();
                return nullptr;
            }
            Job* stolenJob = victimQueue->steal_job();
            if (is_job_empty(stolenJob)) {
                _yield();
                return nullptr;
            }
            return stolenJob;
        }
        return job;
    }
    
    void threadloop() {
        while (m_is_running) {
            Job* job = fetch_job();
            if (job) { execute_job(job); }
        }
    }
    static
    void run(Job* job) {
        job_stealing_worker* queue = get_worker();
        queue->push_job(job);
    }
    static
    void wait(const Job* job) {
        while (!has_job_completed(job)) {
            Job* nextJob = fetch_job();
            if (nextJob) { execute_job(nextJob); }
        }
    }

    static
    Job* allocate_job() {
        const uint32 index = ++g_allocatedJobs;
        return &g_jobAllocator[(index-1u) & g_mask];
    }

    static
    Job* create_job(JobFunction function) {
        Job* job = allocate_job();
        job->function = function;
        job->parent = nullptr;
        job->unfinishedJobs = 1;        
        return job;
    }

    static
    Job* create_job_as_child(Job* parent, JobFunction function) {
        _atomic_increment(&parent->unfinishedJobs);
        Job* job = allocate_job();
        job->function = function;
        job->parent = parent;
        job->unfinishedJobs = 1;
        return job;
    }

    inline
    static
    void finish_job(Job* job) { 
        _atomic_decrement(&job->unfinishedJobs);       
        if ((job->unfinishedJobs == 0) && (job->parent)) { finish_job(job->parent); }
    }

    inline
    static
    void execute_job(Job* job) {
        (job->function)(job, job->data);
        job->function = nullptr;              
        finish_job(job);        
    }

    inline
    static
    bool is_job_empty(const Job* job) {
        if (job->function == nullptr) { return true; }
        return false;
    }
    inline
    static
    bool has_job_completed(const Job* job) {
        if (job->unfinishedJobs <= 0) { return true; }
        return false;
    }


    // -- STATIC_THREADFUNC --
    static
    void* thread_function(void*);
    // -- ~STATIC_THREADFUNC --

    void initialize();
    void initialize(uint16);    

    void start_threads();
    void stop_threads();

    scheduler();
    ~scheduler();
    scheduler(scheduler const&)=delete;
    void operator=(scheduler const& other)=delete;
};

// -- THREAD_ARGS --
struct thread_args
{
    scheduler* pscheduler;
    uint16     thread_num;
};
// -- ~THREAD_ARGS --

inline
void logger(void* pargs) {
    scheduler* pscheduler = (scheduler*)pargs;
    std::cout << 
    " running_threads: " << pscheduler->m_running_threads << 
    " active_threads: "  << pscheduler->m_active_threads  << 
    std::endl;  
}



static uint64 calls_count;
void* scheduler::thread_function(void* pargs) {
    thread_args args      = *(thread_args*)pargs;
    uint16 temp_thnum     = args.thread_num;
    scheduler* pscheduler = args.pscheduler;

    tls_thread_num        = temp_thnum;
    tls_worker_id         = tls_thread_num-1u;

    _atomic_increment(&pscheduler->m_active_threads);

    if (pscheduler->m_is_running) {
        for ( uint64 i = 0; i < 32*64u ; ++i) { 
            for ( uint64 i = 0; i < 1024*1024u ; ++i) { 
            }
  
        } 
    }
    
    _atomic_decrement(&pscheduler->m_running_threads);

    return nullptr;
}

void scheduler::initialize() {
    initialize(get_no_logical_cores());
}


void scheduler::initialize(uint16 max_no_cores) {    
    assert(max_no_cores);
    assert(max_no_cores-1u == g_max_no_workers);
    m_cores_available = max_no_cores;
    

    delete[] m_pworker_store;
    m_pworker_store = new job_stealing_worker[g_max_no_workers];    
    
    for (uint16 worker_it = 0; worker_it < g_max_no_workers; ++worker_it) {
        g_jobQueues[worker_it] = &m_pworker_store[worker_it];
    }
}

void scheduler::start_threads() {
    delete[] m_pthread_IDs;
    delete[] m_pthread_store;

    m_pthread_IDs                 = new threadid_t [m_cores_available];
    m_pthread_store               = new thread_args[m_cores_available];

    m_pthread_IDs  [0]            = 0;
    m_pthread_store[0].thread_num = 0;
    m_pthread_store[0].pscheduler = this;

    for (uint16 thread_it = 1; thread_it < m_cores_available; ++thread_it) {

        m_pthread_store[thread_it].thread_num = thread_it;
        m_pthread_store[thread_it].pscheduler = this;

        thread_create(&m_pthread_IDs[thread_it], thread_function, &m_pthread_store[thread_it]); 
        ++m_running_threads;        
        ();
    }
}

void scheduler::stop_threads() {

    m_is_running = ~m_is_running;

    for (uint16 thread_it = 1; thread_it < m_cores_available; ++thread_it) {

        thread_terminate(m_pthread_IDs[thread_it]); 
        --m_active_threads;
        ();
    }

    delete[] m_pthread_IDs;
    delete[] m_pthread_store;
    m_pthread_IDs   = nullptr;
    m_pthread_store = nullptr;
}

scheduler::scheduler() 
    : m_cores_available(1)
    , m_active_threads(1)
    , m_running_threads(1) 

    , m_pworker_store(nullptr)    
    , m_pthread_store(nullptr)
    , m_pthread_IDs(nullptr)
    , m_is_running(true)
{
    memset(&g_jobQueues, 0, sizeof(g_jobQueues));
}

scheduler::~scheduler() {    

    delete[] m_pthread_IDs;
    delete[] m_pthread_store;
    delete[] m_pworker_store;
    m_pthread_IDs   = nullptr;
    m_pthread_store = nullptr;
    m_pworker_store = nullptr;
}
// -- ~SCHEDULER --





int main () {
    scheduler* task_razor = new scheduler;    

    task_razor->initialize();
    task_razor->start_threads();

    // -- wait --
    if (task_razor->m_is_running) {        
        for ( uint64 i = 0; i < 64*64u ; ++i) { 
            for ( uint64 i = 0; i < 1024*1024u ; ++i) { 
            }
        }
    }
    // -- ~wait --

    task_razor->(calls_count);
    task_razor->stop_threads();
    task_razor->(calls_count);
    delete task_razor;
}



