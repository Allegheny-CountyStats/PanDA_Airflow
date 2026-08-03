
# PanDAirflow Unit Testing Standards

This document defines how unit tests must be written for PanDAirflow
operators, sensors, utilities, and API wrappers to ensure consistency,
reliability, and Airflow compatibility.

---

## 1. Test Runner

All tests use:
  - pytest
  - Python >= 3.10
  - Airflow SDK (Airflow >= 3.0)

PyCharm automatically discovers tests matching:
  - File pattern: test_*.py
  - Function pattern: test_*

---

## 2. Directory Layout

```
tests/
│
├── unit/          # Mock-based, no Airflow runtime
│   └── test_x.py
│
└── integration/   # Uses airflow tasks test
    └── test_y.py
```

---

## 3. Required Mocking Rules

Because Airflow SDK requires runtime context for metadata access,
the following objects must always be mocked in unit tests:

### Airflow Variables

```
@patch("airflow.sdk.definitions.variable.Variable.get")
```

### External APIs (MoveIt, HTTP hooks, custom APIs)

```
@patch("PanDAirflow.module.function")
```

### Any database or filesystem access

Use mock or tmpdir fixtures.

Unit tests must never call:

- `auth_move_it` (unless USE_REAL_API=true)
- `available_files` (unless USE_REAL_API=true)
- TaskFlow-decorated functions directly (call `__wrapped__`)

---

## 4. TaskFlow Decorator Handling

Always bypass decorators using:

```
result = operator.__wrapped__(args...)
```

Direct calls return XComArg and break unit tests.

---

## 5. Parameter Injection

Unit tests MUST support parameter injection via environment variables:

```
FILE_NAME=...
FOLDER_PATH=...
FILE_TYPES=.csv,.txt
USE_REAL_API=true/false
MI_USER=...
MI_PASS=...
```

This allows developers to test:

- mock input
- real API input
- multiple scenarios
- without editing test code

---

## 6. Assertions

Tests must assert:

- `result.is_done`
- `result.xcom_value`
- expected logical outcomes
- correct filtering behavior

---

## 7. Real API Mode

Real MoveIt API calls are allowed only when:

- test runner is SSH-based on the Airflow VM
- `USE_REAL_API=true`
- correct MoveIt credentials are provided

Mock mode is default.

---

## 8. CI/CD Strategy

Unit tests → run locally and in GitHub Actions
Integration tests → run on Airflow VM using `airflow tasks test`
