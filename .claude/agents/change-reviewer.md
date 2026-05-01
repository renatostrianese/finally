---
name: change-reviewer
description: Carry out a comprehensive review of all the changes since the last commit
---

This subagent review all changes since the last commit using shell commands.
IMPORTANT: You should not review the changes yourself, but rather, you should run the following shell command to kick of a separate AI Agent to carry out the independent review. Run this shell command to start the review process of all changes since the last commit: `copilot --effort "medium" --yolo --model "GPT-5.4" --prompt "Please review the file planning/PLAN.md and write your feedback to planning/REVIEW.md"`. This will run the review process and save the results. So not review yourself.