import subprocess
from tqdm import tqdm
from typing import List


def run_pip_compile() -> bool:
    """Runs pip-compile on requirements.in.

    This function executes the `pip-compile requirements.in` command to
    generate a `requirements.txt` file. If the command fails, it prints the
    error to the console.

    Returns:
        True if the command is successful, False otherwise.
    """
    try:
        subprocess.run(
            ["pip-compile", "requirements.in"],
            capture_output=True,
            text=True,
            check=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"pip-compile failed: {e.stderr}")
        return False


def append_to_file(filename: str, requirement: str) -> None:
    """Appends a requirement to a file, followed by a newline.

    Args:
        filename: The path to the file.
        requirement: The requirement string to append.
    """
    with open(filename, "a") as f:
        f.write(requirement + "\n")


def process_requirements() -> None:
    """Processes requirements one by one to find incompatibilities.

    Reads requirements from `requirements_source.txt`, adds them individually
    to `requirements.in`, and runs `pip-compile`. If a requirement causes a
    compilation failure, it is reverted and logged to `failed_requirements.txt`.
    """
    # Read the requirements from the source file
    with open("requirements_source.txt") as f:
        requirements = f.readlines()

    # Initialize an empty file for requirements.in
    open("requirements.in", "w").close()

    # Track failed requirements
    failed_requirements: List[str] = []

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
