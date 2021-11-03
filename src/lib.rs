use log::{info, warn};
use proxy_wasm::{traits::Context, types::Status};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static WAITING_CONTEXTS: RefCell<HashMap<u32, Box<dyn UniqueCallouts>>> = RefCell::new(HashMap::new());
}

#[derive(Debug, thiserror::Error)]
pub enum UniqueCalloutError<'a> {
    #[error("failed to serialize {what:?} due to {reason:?}")]
    SerializeFail {
        what: &'a str,
        reason: bincode::ErrorKind,
    },
    #[error("failed to deserialize {what:?} due to {reason:?}")]
    DeserializeFail {
        what: &'a str,
        reason: bincode::ErrorKind,
    },
    #[error("failure due to proxy's internal issue: {0:?}")]
    ProxyFailure(Status),
}

// This struct is serialized and stored in the shared data for callout-lock winner
// to know which thread to wake up and to let waiters know which http context to resume processing.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CalloutWaiter {
    /// MQ id of the thread waiting for callout response.
    pub queue_id: u32,
    /// Id of http context waiting for callout response.
    pub http_context_id: u32,
}

/// This struct is serialized and stored as the value of a callout lock.
#[derive(Debug, Serialize, Deserialize)]
struct CalloutLockValue {
    /// Id of the thread who owns the lock.
    pub owner_id: u32,
    /// List of contexts that are waiting for lock to be freed.
    pub waiters: Vec<CalloutWaiter>,
}

// This enum is passed to thread-specific MQs to let waiters know how to resume processing.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum WaiterAction {
    /// Follow the success path for context with id same as inner value.
    HandleSuccess(u32),
    /// Follow the failure path for context with id same as inner value.
    HandleFailure(u32),
}

// This enum is used to give out status for an http context trying to get callout-lock.
pub enum SetCalloutLockStatus {
    // Callout-lock is acquired by the current context.
    LockAcquired,
    // Current context is added to the callout-waitlist.
    AddedToWaitlist,
    // Before context could be added to waitlist, callout response came first.
    ResponseCameFirst,
}

/** TD;LR on how lock is acquired by exploiting host implementation:
* shared data is essentially a K-V store that maps key to a pair of (value, cas).
*
* set_shared_data(key, value, cas)'s psuedo-code is as follows:
*   lock_using_a_mutex() // This guarantees only 1 thread executes its instructions at a time.
*   if (key is present in the hashmap)
*      if (cas && cas != cas_of_last_value_added)
*          return CasMisMatch
*      update value with new cas
*   else insert_new_entry_into_the_hashmap
*
* A lock is acquired by a thread when it successfully adds an entry of the format:
*              ({CALLOUT_LOCK_KEY}, ({CALLOUT_LOCK_VALUE}, CAS))
*
* If you look at Rust SDK spec, when an entry is not present, you get (None, None) as result.
* Now imagine a scenario, when two threads (T1 & T2) check for the same lock in the store and find no lock.
* T1 & T2 tries to set an entry by calling: set_shared_data(key, value, **None**).
*
* If you go through above-mentioned algo, no matter the order which thread executes first, second thread
* will overwrite the first one because 'None' CAS is translated into 0 in SDK and second-if condition is passed-over.
*
* Now imagine if T1 & T2 use '1'(any but 0) as CAS, first thread will insert a new entry since it's not
* already present in the hashmap and second one will get CasMismatch in the result due to second-if condition.
* NOTE: Since CAS is a u32 integer, after u32::MAX, CAS resets to 1(0 is skipped). What this means for unique-callout
* is that N threads can successfully acquire the lock again when lock is freed with CAS=1. But the chances
* of this happening is worse than winning a lottery and *your code should handle this case as well*.
*
* It's good to point that 'None' as CAS was intended for overwriting the exisiting value and that's how
* we free the lock actually, by sending 'None' as value and CAS.
*
* Please read host implementation (shared_data.cc/h) and Rust SDK/hostcalls.rs for better understanding.
**/

pub trait UniqueCallouts: Context {
    /// key used to check uniqueness.
    fn callout_lock_key(&self) -> String;

    /// uniquely identifies running thread trying to acquire the lock.
    fn thread_id(&self) -> u32;

    /// id of queue registered by the running thread. // TODO: document the behavior exploited here.
    fn queue_id(&self) -> u32;

    /// returns clone of the struct implementing this trait.
    fn clone(&self) -> Box<dyn UniqueCallouts>;

    /// callout lock is acquired by placing a key-value pair inside shared data.
    /// * `context_id` - id of context that receives the callout response.
    fn set_callout_lock(
        &self,
        context_id: u32,
    ) -> Result<SetCalloutLockStatus, UniqueCalloutError> {
        let callout_lock_key = self.callout_lock_key();
        let queue_id = self.queue_id();
        let thread_id = self.thread_id();

        info!(
            "thread(id: {}): trying to set callout-lock for request(key: {})",
            thread_id, callout_lock_key
        );

        let callout_lock_value = CalloutLockValue {
            owner_id: thread_id,
            waiters: Vec::new(),
        };

        // just in case lock by another context, this context will be added to waiters.
        let callout_waiter = CalloutWaiter {
            queue_id,
            http_context_id: context_id,
        };

        let lock_value_serialized =
            match bincode::serialize::<CalloutLockValue>(&callout_lock_value) {
                Ok(res) => res,
                Err(e) => {
                    return Err(UniqueCalloutError::SerializeFail {
                        what: "CalloutLockValue",
                        reason: *e,
                    })
                }
            };

        let mut last_seen_lock_owner: Option<u32> = None;
        loop {
            // check if lock is already acquired or not
            match self.get_shared_data(&callout_lock_key) {
                (_, None) => {
                    info!(
                        "thread ({}): trying to acquire lock ({}) the first time",
                        thread_id, callout_lock_key
                    );
                    // Note: CAS is not 'None' here                                             ∨∨∨∨∨∨
                    match self.set_shared_data(
                        &callout_lock_key,
                        Some(&lock_value_serialized),
                        Some(1),
                    ) {
                        Ok(()) => {
                            info!(
                                "thread ({}): callout-lock ({}) acquired",
                                thread_id, callout_lock_key
                            );
                            return Ok(SetCalloutLockStatus::LockAcquired);
                        }
                        Err(Status::CasMismatch) => {
                            info!(
                                "thread ({}): callout-lock ({}) for already acquired by another thread",
                                thread_id,
                                callout_lock_key
                            );
                            // Now try to add it to waitlist instead.
                            continue;
                        }
                        Err(e) => return Err(UniqueCalloutError::ProxyFailure(e)),
                    }
                }
                (Some(bytes), Some(cas)) => {
                    let mut stored_lock_value =
                        match bincode::deserialize::<CalloutLockValue>(&bytes) {
                            Ok(res) => res,
                            Err(e) => {
                                return Err(UniqueCalloutError::DeserializeFail {
                                    what: "CalloutLockValue",
                                    reason: *e,
                                })
                            }
                        };
                    info!(
                        "thread ({}): callout-lock ({}) already acquired by thread ({}), trying to add to waitlist",
                        thread_id,
                        callout_lock_key,
                        stored_lock_value.owner_id
                    );
                    stored_lock_value.waiters.push(callout_waiter.clone());
                    let updated_lock_value_serialized =
                        match bincode::serialize::<CalloutLockValue>(&stored_lock_value) {
                            Ok(res) => res,
                            Err(e) => {
                                return Err(UniqueCalloutError::SerializeFail {
                                    what: "(updated) CalloutLockValue",
                                    reason: *e,
                                })
                            }
                        };
                    if let Err(Status::CasMismatch) = self.set_shared_data(
                        &callout_lock_key,
                        Some(&updated_lock_value_serialized),
                        Some(cas),
                    ) {
                        last_seen_lock_owner = Some(stored_lock_value.owner_id);
                        continue;
                    }
                    info!(
                        "thread({}): added to waitlist for callout-lock({})",
                        thread_id, callout_lock_key
                    );
                    WAITING_CONTEXTS
                        .with(|waiters| waiters.borrow_mut().insert(context_id, self.clone()));
                    return Ok(SetCalloutLockStatus::AddedToWaitlist);
                }
                (None, Some(cas)) => {
                    if last_seen_lock_owner.is_some() {
                        // this can happen when callout response come before context
                        // is able to add itself to the waitlist.
                        info!(
                            "thread({}): lock was freed before context was added to waitlist",
                            thread_id
                        );
                        return Ok(SetCalloutLockStatus::ResponseCameFirst);
                    }
                    info!(
                        "thread ({}): callout-lock ({}) was already free and trying to acquire again",
                        thread_id,
                        callout_lock_key
                    );
                    match self.set_shared_data(
                        &callout_lock_key,
                        Some(&lock_value_serialized),
                        Some(cas),
                    ) {
                        Ok(()) => {
                            info!(
                                "thread ({}): callout-lock ({}) successfully acquired",
                                thread_id, callout_lock_key
                            );
                            return Ok(SetCalloutLockStatus::LockAcquired);
                        }
                        Err(Status::CasMismatch) => {
                            info!(
                                "thread ({}): callout-lock ({}) already acquired by another thread",
                                thread_id, callout_lock_key
                            );
                            // Now try to add context to waitlist.
                            continue;
                        }
                        Err(e) => return Err(UniqueCalloutError::ProxyFailure(e)),
                    }
                }
            }
        }
    }

    /// Callout-lock is freed by setting null value in the shared data.
    /// NOTE: Right now, there is no option of deleting the pair instead only the value can be erased,
    /// and it requires changes in the ABI so change this because it will then lead to better memory usage.
    fn free_callout_lock_and_notify_waiters(
        &self,
        mut waiter_action: WaiterAction,
    ) -> Result<(), UniqueCalloutError> {
        let callout_lock_key = self.callout_lock_key();
        let thread_id = self.thread_id();
        info!(
            "thread ({}): trying to free callout-lock and notify waiters({})",
            thread_id, callout_lock_key
        );

        match self.get_shared_data(&callout_lock_key) {
            (Some(bytes), _) => {
                let callout_lock_value = match bincode::deserialize::<CalloutLockValue>(&bytes) {
                    Ok(res) => res,
                    Err(e) => {
                        return Err(UniqueCalloutError::DeserializeFail {
                            what: "CalloutLockValue",
                            reason: *e,
                        })
                    }
                };
                for callout_waiter in callout_lock_value.waiters {
                    match waiter_action {
                        WaiterAction::HandleFailure(ref mut ctxt_id) => {
                            *ctxt_id = callout_waiter.http_context_id
                        }
                        WaiterAction::HandleSuccess(ref mut ctxt_id) => {
                            *ctxt_id = callout_waiter.http_context_id
                        }
                    }
                    let message = match bincode::serialize::<WaiterAction>(&waiter_action) {
                        Ok(res) => res,
                        Err(e) => {
                            warn!(
                                "failed to serialize WaiterAction {:?}: {}",
                                waiter_action, e
                            );
                            continue; // one serialize fail should not prevent others to try.
                        }
                    };
                    if let Err(e) =
                        self.enqueue_shared_queue(callout_waiter.queue_id, Some(&message))
                    {
                        // There is nothing we can do to signal other threads now and should just
                        // allow them to timeout and maybe add another mechanism for memory clearance.
                        warn!(
                            "thread({}): enqueue failure for queue({}): {:?}",
                            thread_id, callout_waiter.queue_id, e
                        );
                        warn!(
                            "failed to enqueue message to notify waiter({:?})",
                            callout_waiter
                        );
                        continue; // one enqueue fail should not prevent others to try.
                    }
                }
                // Note: safe for current SDK implementation.
                self.set_shared_data(&callout_lock_key, None, None).unwrap();
            }
            (None, _) => {
                // This can happen either when another thread freed waiting contexts or there was only
                // one request for this specific application. If this happens for the former,
                // check implementation of acquiring and freeing lock as it's not supposed to happen.
                warn!(
                    "thread ({}): trying to free non-existing callout-lock ({})",
                    thread_id, callout_lock_key
                );
            }
        }
        Ok(())
    }
}
