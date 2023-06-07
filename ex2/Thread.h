#ifndef EX2_THREAD_H
#define EX2_THREAD_H

#include <csetjmp>
#include "uthreads.h"




//#include "uthreads.h"

enum STATE
{
    RUNNING, READY, BLOCKED
};
typedef void (*thread_entry_point)(void);


class Thread
{
 private:
  int _id;
  int _running_quantums;
  STATE _state;
    sigjmp_buf env;
    char _stack[STACK_SIZE]{};
  thread_entry_point _entry_point;

 public:
  Thread (int id, STATE state, thread_entry_point entry_point);

  int get_running_quantums () const;

  void increase_running_quantums ();

  int get_id ();

  STATE get_state ();

  void set_state (STATE new_state);

  char* get_stack ();

  sigjmp_buf& get_env(){
      return env;
    }
  thread_entry_point get_entry_point ();

    void
    setup_thread(int tid, const char *stack, thread_entry_point entry_point);
};

#endif //EX2_THREAD_H
