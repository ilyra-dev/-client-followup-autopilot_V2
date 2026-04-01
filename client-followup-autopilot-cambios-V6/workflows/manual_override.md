# Manual Override Procedures

## Objective
Allow CS team members to control the Follow-Up Autopilot at the item level, pausing, resuming, or modifying follow-up sequences as needed.

---

## Pause Follow-Ups for a Specific Item

1. Open the item in Notion
2. Check the **Manual Override** checkbox
3. The autopilot will skip this item in all future cycles
4. A note will appear in the daemon log: "Skipped [item] — Manual Override active"

---

## Resume Follow-Ups

1. Open the item in Notion
2. Uncheck **Manual Override**
3. Optionally adjust:
   - **Follow-Up Stage**: Set to the stage you want to resume from (0-3)
   - **Next Follow-Up Date**: Set to when you want the next follow-up to go out
4. The item will be picked up in the next outbound cycle

---

## Change Follow-Up Timing

To delay or advance a follow-up:
1. Edit **Next Follow-Up Date** to the desired date
2. The system will send the next follow-up on that date instead of the calculated one

---

## Skip a Stage

To skip directly to a later stage (e.g., jump from Stage 1 to Stage 3):
1. Set **Follow-Up Stage** to the stage BEFORE the one you want to send next
   - Example: Set to 2 to make the next send be Stage 3
2. Set **Next Follow-Up Date** to today or desired date
3. Uncheck **Manual Override** if it was checked

---

## Force Stop All Follow-Ups

To permanently stop follow-ups for an item:
1. Set **Status** to "Received" or "Paused"
2. Check **Manual Override** (belt and suspenders)
3. Add a note in **Follow-Up Log** explaining why

---

## Change Client Contact

To redirect follow-ups to a different email:
1. Update **Client Email** in Notion
2. For Stage 4 escalation, update **Senior Contact Email**
3. Changes take effect on the next follow-up cycle

---

## Change Language

1. Change **Client Language** select to ES, EN, or PT
2. Next generated email will be in the new language

---

## Emergency: Stop All Outbound Emails

If the system is sending incorrect emails:
1. Stop the daemon: `Ctrl+C` in the terminal or `kill <PID>`
2. Or set `SYSTEM_MODE=DRAFT` in `.env` to switch to draft-only mode
3. Review and fix the issue
4. Restart the daemon

---

## Notes for CS Team
- All manual changes in Notion are respected on the next cycle (max wait: 30 minutes)
- Manual Override is the safest way to pause — it's checked before every action
- The Follow-Up Log shows everything the system has done and why
- You can always add notes to the Follow-Up Log field; the system appends but never overwrites your notes
