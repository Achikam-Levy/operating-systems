#ifndef EX_2_THREAD_CPP
#include <iostream>
#include <csignal>
#include "Thread.h"

#define EX_2_THREAD_CPP
#ifdef __x86_64__
/* code for 64-bit Intel arch */

typedef unsigned long address_t;
#define JB_SP 6

#define JB_PC 7

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address (address_t addr)
{
  address_t ret;
  asm volatile("xor    %%fs:0x30,%0\n"
               "rol    $0x11,%0\n"
      : "=g" (ret)
      : "0" (addr));
  return ret;
}
#else
/* code for 32-bit Intel arch */

typedef unsigned int address_t;
#define JB_SP 4
#define JB_PC 5

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address (address_t addr)
{
  address_t ret;
  asm volatile("xor    %%gs:0x18,%0\n"
               "rol    $0x9,%0\n"
      : "=g" (ret)
      : "0" (addr));
  return ret;
}


#endif

Thread::Thread (int id, STATE state, thread_entry_point entry_point)
    : _id (id), _running_quantums (0), _state (state)
{
  if (id != 0)
  {
      _entry_point = entry_point;
  }
}

int Thread::get_running_quantums () const
{
  return _running_quantums;
}

void Thread::increase_running_quantums ()
{
  _running_quantums++;
}

int Thread::get_id ()
{ return _id; }

STATE Thread::get_state ()
{
    return _state; }

void Thread::set_state (STATE new_state)
{
  _state = new_state;
}

char* Thread::get_stack ()
{
  return _stack;
}

thread_entry_point Thread::get_entry_point ()
{
  return _entry_point;
}

void Thread::setup_thread (int tid, const char* stack, thread_entry_point
entry_point)
{
    // initializes env[tid] to use the right stack, and to run from the
    // function 'entry_point', when we'll use
    // siglongjmp to jump into the thread.
    address_t sp = (address_t) stack + STACK_SIZE - sizeof (address_t);
    auto pc = (address_t) entry_point;
    sigsetjmp (env, 1);
    (env->__jmpbuf)[JB_SP] = translate_address (sp);
    (env->__jmpbuf)[JB_PC] = translate_address (pc);
    sigemptyset (&env->__saved_mask);
}

#endif //EX_2_THREAD_CPP
