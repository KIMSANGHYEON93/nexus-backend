```markdown
# nexus-backend Development Patterns

> Auto-generated skill from repository analysis

## Overview
This skill teaches the core development patterns and conventions used in the `nexus-backend` Python codebase. You'll learn how to structure files, write and organize code, follow commit message conventions, and understand the project's approach to testing. This guide is ideal for contributors aiming to maintain consistency and quality in the repository.

## Coding Conventions

### File Naming
- Use **snake_case** for all file names.
  - Example: `user_service.py`, `data_processor.py`

### Import Style
- Use **relative imports** within the package.
  - Example:
    ```python
    from .models import User
    from .utils import process_data
    ```

### Export Style
- Use **named exports** (i.e., explicitly define what is exported from a module).
  - Example:
    ```python
    # In user_service.py
    def create_user(...):
        ...

    def delete_user(...):
        ...

    __all__ = ["create_user", "delete_user"]
    ```

### Commit Message Convention
- Follow the **Conventional Commits** specification.
- Use the `feat` prefix for new features.
- Commit message should be concise (average 66 characters).
  - Example:
    ```
    feat: add user authentication to login endpoint
    ```

## Workflows

### Adding a New Feature
**Trigger:** When implementing a new feature or endpoint  
**Command:** `/add-feature`

1. Create a new Python module with a snake_case filename.
2. Use relative imports to include any shared utilities or models.
3. Define functions or classes, and list them in `__all__` for named exports.
4. Write or update tests in a corresponding `*.test.*` file.
5. Commit your changes using the conventional commit format:
    ```
    feat: short description of the feature
    ```
6. Push your branch and open a pull request.

### Writing Tests
**Trigger:** When adding or updating functionality  
**Command:** `/write-test`

1. Create or update a test file matching the pattern `*.test.*` (e.g., `user_service.test.py`).
2. Write test cases for new or modified functions/classes.
3. Use the project's preferred (unknown) testing framework.
4. Run tests to ensure all pass before committing.

## Testing Patterns

- Test files follow the `*.test.*` naming pattern (e.g., `module.test.py`).
- The specific testing framework is not detected; follow existing patterns in the repo.
- Place tests alongside or near the modules they test.

**Example:**
```python
# user_service.test.py

from .user_service import create_user

def test_create_user():
    user = create_user("test@example.com")
    assert user.email == "test@example.com"
```

## Commands
| Command        | Purpose                                      |
|----------------|----------------------------------------------|
| /add-feature   | Guide for adding a new feature or endpoint   |
| /write-test    | Steps for writing and organizing tests       |
```
