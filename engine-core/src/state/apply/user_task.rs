//! User-task-family appliers.

use crate::event::Event;
use crate::state::types::*;

pub(super) fn apply_user_task(state: &mut State, event: &Event) {
    match event {
        Event::UserTaskTransitionDeferred {
            user_task_key,
            pending,
            ..
        } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                task.pending = Some(pending.clone());
            }
        }

        Event::UserTaskCorrectionsApplied {
            user_task_key,
            corrections,
            ..
        } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                if let Some(pending) = task.pending.as_mut() {
                    pending.corrections.merge(corrections);
                }
            }
        }

        Event::UserTaskTransitionResolved { user_task_key, .. } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                task.pending = None;
            }
        }

        Event::UserTaskCreated {
            user_task_key,
            instance_key,
            element_instance_key,
            element_id,
            created_at,
            assignee,
            candidate_groups,
            candidate_users,
            due_date,
            follow_up_date,
            priority,
            form_key,
            external_form_reference,
        } => {
            state.user_tasks.insert(
                *user_task_key,
                UserTask {
                    key: *user_task_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    state: UserTaskState::Created,
                    assignee: assignee.clone(),
                    candidate_groups: candidate_groups.clone(),
                    candidate_users: candidate_users.clone(),
                    due_date: due_date.clone(),
                    follow_up_date: follow_up_date.clone(),
                    priority: *priority,
                    form_key: *form_key,
                    external_form_reference: external_form_reference.clone(),
                    created_at: *created_at,
                    pending: None,
                },
            );
        }

        Event::UserTaskAssigned {
            user_task_key,
            assignee,
            ..
        } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                task.assignee = assignee.clone();
            }
        }

        Event::UserTaskUpdated {
            user_task_key,
            candidate_groups,
            candidate_users,
            due_date,
            follow_up_date,
            priority,
            ..
        } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                if let Some(groups) = candidate_groups {
                    task.candidate_groups = groups.clone();
                }
                if let Some(users) = candidate_users {
                    task.candidate_users = users.clone();
                }
                if let Some(due) = due_date {
                    task.due_date = due.clone();
                }
                if let Some(follow_up) = follow_up_date {
                    task.follow_up_date = follow_up.clone();
                }
                if let Some(p) = priority {
                    task.priority = *p;
                }
            }
        }

        Event::UserTaskCompleted { user_task_key, .. } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                task.state = UserTaskState::Completed;
            }
        }

        Event::UserTaskCanceled { user_task_key, .. } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                task.state = UserTaskState::Canceled;
                // A cancelled task has no in-flight transition: its listener job
                // (if any) was cancelled with the instance's other jobs. Clearing
                // pending keeps replay from reconstructing a terminal task with a
                // permanently unresolved transition. No-op for listener-free tasks
                // (pending is already None), so the journal stays byte-identical.
                task.pending = None;
            }
        }
        _ => unreachable!(),
    }
}
