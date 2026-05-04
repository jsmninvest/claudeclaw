# Anti-Idle v5: Plan → Codex → Build Workflow

## Architecture

### Chain (every task, no exceptions)
```
Anti-idle cron (notifier only)
  → Main (chief of staff, triages + assigns domain expert)
    → Domain Expert (plans + runs Codex review)
      → Builder (executes Codex-approved plan only)
        → Domain Expert or QA (verifies completion)
```

### Agent Roles

| Agent | Role | Never Does |
|-------|------|------------|
| Anti-idle cron | Scans kanban for idle `todo` tasks. Creates durable `triage_idle_kanban` mission tasks to Main. Upserts (no duplicates). | Route, plan, assign domain experts, create missions to anyone except Main |
| Main (Rudy) | Chief of staff. Reads triage missions, determines domain expert, delegates via mission task. | Execute code, plan domain-specific work |
| Research | Plans SEO/AEO/market/competitor tasks. Runs Codex on own plan. | Write code |
| Comms | Plans client-facing communication (email, SMS, realtor outreach). Runs Codex on own plan. | Write code |
| Content | Plans marketing content (blog, TikTok, social, newsletters). Runs Codex on own plan. | Write code |
| Ops | Plans infrastructure/cron/monitoring tasks. Runs Codex on own plan. | Write code (delegates to builder) |
| Builder | Executes Codex-approved plans ONLY. May reject plans with structured reason. | Plan, route, run Codex, accept tasks without a Codex-approved plan |
| QA | Verifies completion for client-facing/content/high-risk tasks. | Plan, build |

### Workflow States (new column: `workflow_state` on kanban_tasks)

```
todo                    -- idle, waiting for anti-idle to notice
triage_requested        -- anti-idle created mission to Main
triaged                 -- Main assigned a domain expert
planning                -- domain expert is creating a plan
review_requested        -- plan submitted to Codex
review_passed           -- Codex PASS, ready for builder
review_conditional      -- Codex CONDITIONAL PASS, expert resolving
review_failed           -- Codex FAIL, expert must re-plan
build_requested         -- approved plan sent to builder
building                -- builder executing
builder_rejected        -- builder returned structured rejection
verification_requested  -- builder done, awaiting domain/QA check
done                    -- verified complete
externally_blocked      -- waiting on human/external input
```

Each state records:
- `workflow_owner` -- current responsible agent
- `workflow_correlation_id` -- links all missions for this task
- `workflow_attempt` -- count of plan→review cycles (max 2 before escalate)
- `workflow_updated_at` -- timestamp of last state change

### DB Schema Changes

```sql
-- Add workflow columns to kanban_tasks
ALTER TABLE kanban_tasks ADD COLUMN workflow_state TEXT DEFAULT NULL;
ALTER TABLE kanban_tasks ADD COLUMN workflow_owner TEXT DEFAULT NULL;
ALTER TABLE kanban_tasks ADD COLUMN workflow_correlation_id TEXT DEFAULT NULL;
ALTER TABLE kanban_tasks ADD COLUMN workflow_attempt INTEGER DEFAULT 0;
ALTER TABLE kanban_tasks ADD COLUMN workflow_updated_at INTEGER DEFAULT NULL;

CREATE INDEX idx_kanban_workflow ON kanban_tasks(workflow_state, workflow_updated_at);
```

### Anti-Idle Cron Changes (anti-idle-check.mjs + anti-idle-orchestrator.mjs)

**Before:** Scans todo tasks → keyword-routes → creates mission to domain agent
**After:** Scans todo tasks → upserts `triage_idle_kanban` mission to Main

Key behaviors:
1. Only creates missions assigned to `main`
2. Upserts by kanban task ID (if an active triage mission already exists for this task, skip)
3. Sets `workflow_state = 'triage_requested'` on the kanban task
4. Includes in mission payload: task ID, title, priority, age, blocked flag, tags, description
5. Sends Telegram summary: "N idle tasks queued for triage"
6. Does NOT route, does NOT decide domain expert
7. SLA: if `triage_requested` is untouched for 30 min, re-notify Main

**Idempotency:** Check `mission_tasks` for existing queued/running task with same kanban_task_id before creating. Use `workflow_correlation_id` as dedup key.

### Main Triage Protocol

When Main receives a `triage_idle_kanban` mission:

1. Read the kanban task fully (title, description, notes, tags, history)
2. Determine if task is genuinely actionable or should be `externally_blocked`
3. If actionable, assign primary domain expert based on context reasoning (NOT keyword matching)
4. Create mission to the domain expert with:
   - Kanban task ID
   - Full context
   - Expected outcome (from task description or Main's interpretation)
   - Instruction: "Create a plan, run Codex adversarial review, then delegate to builder"
5. Set `workflow_state = 'triaged'`, `workflow_owner = '<expert_agent>'`
6. If ambiguous, Main may ask Aditya for clarification before assigning

### Domain Expert Protocol

When expert receives a planning mission:

1. Set `workflow_state = 'planning'`
2. Create a plan with:
   - What to do (specific steps)
   - Why (outcome/business value)
   - Acceptance criteria (how to know it's done)
   - Risks/blockers identified
   - Files/systems affected
3. Submit plan to Codex for adversarial review
4. Set `workflow_state = 'review_requested'`
5. Handle Codex response:
   - **PASS**: Set `workflow_state = 'review_passed'`, create mission to builder with approved plan
   - **CONDITIONAL PASS**: Resolve conditions, re-submit if material. Max 2 attempts.
   - **FAIL**: Re-plan from scratch. Max 2 attempts, then escalate to Main.
   - **ESCALATE**: Return to Main with context
6. Set `workflow_state = 'build_requested'` when handing to builder

### Builder Protocol

When builder receives an execution mission:

1. Verify the plan has `workflow_state = 'review_passed'` or `'build_requested'`
2. Verify plan freshness (workflow_updated_at within last 4 hours)
3. Set `workflow_state = 'building'`
4. Execute the approved plan
5. On completion:
   - For code/infra tasks: set `workflow_state = 'done'`, update `column_id = 'done'`
   - For client-facing/content tasks: set `workflow_state = 'verification_requested'`, notify domain expert or QA
6. On rejection (bad plan, missing input, impossible): set `workflow_state = 'builder_rejected'` with structured reason:
   ```json
   {
     "reason": "missing_acceptance_criteria|stale_plan|impossible_command|security_risk|already_done|external_blocker",
     "details": "...",
     "recommended_next_agent": "content"
   }
   ```
   Task returns to domain expert, not Main (unless ownership disputed).

### Blocked Task Handling

Anti-idle treats blocked tasks differently:
- If `column_id = 'blocked'` AND blocker unchanged: update reminder timestamp only, do NOT re-triage
- If `column_id = 'blocked'` AND blocker may have resolved (age > 7 days): create triage mission to Main for re-evaluation
- Blocked tasks with `workflow_state = 'externally_blocked'` are NEVER auto-triaged

### Safety Controls

1. **Max re-plan attempts**: 2 per task per cycle. After that, escalate to Main.
2. **Idempotency**: correlation IDs prevent duplicate missions for the same kanban task
3. **SLA timers**: 
   - Triage: 30 min
   - Planning: 60 min  
   - Codex review: 15 min
   - Build pickup: 30 min
4. **No circular delegation**: task has one `workflow_owner` at a time. Ownership transfer requires reason code.
5. **Builder rejection path**: structured rejections go to domain expert, not Main (unless 2+ rejections)
6. **Stale plan check**: builder verifies plan age before executing

### Files to Modify

1. `scripts/anti-idle-check.mjs` -- Remove all keyword routing logic. Output only: "here are idle tasks, assign to main"
2. `scripts/anti-idle-orchestrator.mjs` -- Create missions to `main` only. Add upsert/dedup logic. Add workflow_state updates.
3. DB migration: add workflow columns to `kanban_tasks`
4. Each agent's CLAUDE.md: add the domain expert protocol (plan → codex → delegate to builder)

### What NOT To Change

- Mission task infrastructure (SQLite queue, 60s pickup) -- works fine
- Codex CLI invocation pattern -- works fine  
- Anti-idle cron schedule (every 2h, 9am-7pm) -- keep as is
- Kanban column_id semantics (todo/inprogress/blocked/done) -- keep, workflow_state is additive
