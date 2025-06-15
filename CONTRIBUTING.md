# Contributing to qdrant-vsql

Thank you for your interest in contributing to qdrant-vsql! We welcome contributions of all kinds. This guide will help you get set up and submit your changes.

## Getting Started

Follow these steps to prepare your development environment:

1.  **Clone the Repository:**

    ```bash
    git clone https://github.com/rcarlomagno/qdrant-vsql
    cd qdrant-vsql
    ```

2.  **Create a Virtual Environment:**

    It's highly recommended to use a virtual environment to isolate the project's dependencies. Choose either `venv` or `conda`:

    **Option 1: Using `venv` (Recommended for most users)**

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate
    ```

    **Option 2: Using `conda`**

    ```bash
    conda create --prefix .venv python=3.9
    conda activate $(pwd)/.venv
    ```
    To delete the conda virtual environment:
    ```bash
    conda env remove -p $(pwd)/.venv
    ```

3.  **Install Dependencies:**

    Install the development dependencies and the project itself in editable mode.

    ```bash
    pip install -r requirements_dev.txt
    pip install -e .
    ```

4. (Optional) **Resolving `AssertionError: Multiple .egg-info directories found`:**
    If you encounter the following error:
    `AssertionError: Multiple .egg-info directories found`

    run:
    ```bash
    find . -name '*.egg-info' -exec rm -rf {} +
    ```

## Contributing Workflow

Here's the standard workflow for contributing:

1.  **Discuss Your Idea (Optional but Encouraged):**
    *   If you're working on something substantial, consider opening an issue to discuss it before you start coding. This helps ensure your efforts align with the project's goals.
2.  **Fork the Repository:**
    *   Click the "Fork" button on the GitHub repository page to create your copy.
3.  **Create a Branch:**
    *   Create a new branch for your work. Give it a descriptive name related to the feature or fix (e.g., `fix-typo-in-readme`, `add-new-functionality`).
    ```bash
    git checkout -b your-branch-name
    ```
4.  **Implement Your Changes:**
    *   Write your code.
5.  **Test Your Changes:**
    *   Ensure your code works as expected. Write unit tests if applicable.
    *   Make sure all the project tests pass.
6.  **Commit Your Work:**
    *   Use clear and concise commit messages that explain the purpose of your changes. Follow the current style already present in the repo.
    ```bash
    git add .
    git commit -m "Fix: Describe the fix in a clear way"
    ```
7.  **Push to Your Fork:**
    ```bash
    git push origin your-branch-name
    ```
8.  **Open a Pull Request (PR):**
    *   Go to the main repository on GitHub.
    *   You'll see a prompt to open a pull request for your recently pushed branch.
    *   Provide a clear description of your changes in the PR description.

## Coding Style

*   Please follow the existing coding style within the project. Consistency is important.

## Commit Messages

*   Write clear, concise, and descriptive commit messages. A good commit message helps others (and your future self) understand the purpose of your changes.

## Testing

*   Thoroughly test your code.
*   If adding a new feature or functionality, include unit tests.
*  Run all the existing tests and make sure they are passing.

## Where to Start

*   Check the "Issues" tab on GitHub for open issues. These often have tasks that are ready for contribution.
*   Refer to the main `README.md` for more details on the project itself.

We appreciate your contributions! Please reach out if you have any questions.
