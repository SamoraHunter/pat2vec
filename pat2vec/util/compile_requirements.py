import subprocess
from tqdm import tqdm


def run_pip_compile():
    """
    Runs pip-compile command and returns True if successful, False otherwise.

    This function is used to run the pip-compile command which generates a
    requirements.txt file from the requirements.in file. If the command is
    successful, it returns True, otherwise it returns False.

    The command is run with the following options:
    - capture_output=True: This ensures that any output from the command is
        captured and returned.
    - text=True: This ensures that the output is returned as a string.
    - check=True: This raises an error if the command fails.

    If the command fails, the error message is printed to the console.
    """
    try:
        result = subprocess.run(
            ["pip-compile", "requirements.in"],
            capture_output=True,
            text=True,
            check=True,  # Raises an error if pip-compile fails
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"pip-compile failed: {e.stderr}")
        return False


def append_to_file(filename, requirement):
    """
    Appends a requirement to a file.
    """
    with open(filename, "a") as f:
        f.write(requirement + "\n")


def process_requirements():
    # Read the requirements from the source file
    with open("requirements_source.txt") as f:
        requirements = f.readlines()

    # Initialize an empty file for requirements.in
    open("requirements.in", "w").close()

    # Track failed requirements
    failed_requirements = []

    # Wrap the requirements in tqdm for progress tracking
    for requirement in tqdm(requirements, desc="Processing Requirements", unit="req"):
        requirement = requirement.strip()
        if not requirement:
            continue  # Skip empty lines

        print(f"\nAdding requirement: {requirement}")

        # Append the requirement to requirements.in
        append_to_file("requirements.in", requirement)

        # Run pip-compile
        if run_pip_compile():
            print(f"Successfully compiled with {requirement}")
        else:
            print(f"Failed to compile with {requirement}, skipping")
            failed_requirements.append(requirement)

            # Revert by removing the last requirement (remove the last line from requirements.in)
            with open("requirements.in", "r") as f:
                lines = f.readlines()
            with open("requirements.in", "w") as f:
                f.writelines(lines[:-1])

    # Write the failed requirements to a file
    if failed_requirements:
        with open("failed_requirements.txt", "w") as f:
            for failed in failed_requirements:
                f.write(failed + "\n")

    print("\nProcessing completed.")
    if failed_requirements:
        print(
            f"Failed requirements saved in failed_requirements.txt: {failed_requirements}"
        )
    else:
        print("All requirements were successfully added and compiled.")


# if __name__ == "__main__":
#     process_requirements()
