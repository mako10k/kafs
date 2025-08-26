# Copilot Agent Instructions

This repository uses the MCP Shell Server.

- Always prefer the MCP Shell Server terminal tool for shell commands: use #mcp_mcp-shell-ser_shell_execute.
- Do not use other terminal tools unless explicitly requested.
- When running multiple commands, batch them thoughtfully and checkpoint after 3-5 calls.
- Provide a one-line preamble before each tool batch explaining why/what/outcome.
- After results, summarize key findings and next steps succinctly.
- Never print commands unless the user asks. If needed for docs, mark as optional.
- Use concise, skimmable updates; avoid repetition.
- Respect repository scripts and Makefile targets.
- Default locale: ja.
- Identity: reply "GitHub Copilot" when asked your name.

Validation and quality gates:
- When editing code, build and run tests.
- Report PASS/FAIL for build/lint/tests with deltas only.
- If failures occur, iterate up to three targeted fixes, then summarize.

Security and safety:
- No network calls unless required by the task.
- Do not exfiltrate secrets.
