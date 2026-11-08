# Risk register and mitigations

| Risk | Impact | Probability | Mitigation / trigger |
|---|---|---|---|
| Four courses consume all available hours | High | High | Target only the relevant lessons; 70% of effort on code/feedback |
| Ray/PyTorch local version or ARM compatibility issue | High | Medium | Pin environment, test small CPU examples, use documented alternative/container when needed |
| PostgreSQL/infra debugging takes too long | Medium | Medium | Keep in-memory adapter for API contract tests; timebox DB integration |
| No local GPU hardware | Low for MVP, high for GPU stretch | High | CPU training + validated GPU manifests; optional limited paid GPU session |
| Too much infrastructure before end-to-end demo | High | High | Gate B is higher priority than P1 polish |
| Worker restart does not truly restore checkpoint | High | Medium | Persist optimizer/epoch state; prove a new process resumes past epoch N |
| Duplicate Ray executions after controller crash | High | Medium | Submission IDs, leases, reconciliation and idempotent commit |
| Hard multi-tenancy isolation not achieved in short scope | Medium | Medium | Test app/tenant boundary; explicitly defer untrusted arbitrary code isolation |
| Interview scheduled before 21 days | High | Medium | Switch to 14-day compression, prioritize mocks and CPU E2E |
| Confusing historical DataRobot tools with new capstone tools | High | Medium | Evidence labels and verified STAR stories; no invented claims |
| Live GPU or cloud bills grow unexpectedly | Medium | Medium | Timebox paid sessions, configure spending alerts, stop instances after tests |

## Review cadence

Review risks on Days 5, 10, 14 and 19. If Gate B slips, suspend P1/P2 and focus on the shortest reliable CPU end-to-end path.
