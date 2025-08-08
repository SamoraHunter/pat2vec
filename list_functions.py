import os
import ast


def find_all_functions(package_path="pat2vec"):
    """
    Walks through a package directory and finds all function definitions.
    """
    function_names = set()  # Use a set to automatically handle duplicates

    for root, _, files in os.walk(package_path):
        for file_name in files:
            if file_name.endswith(".py") and not file_name.startswith("__"):
                file_path = os.path.join(root, file_name)

                with open(file_path, "r", encoding="utf-8") as f:
                    try:
                        # Parse the file content into an abstract syntax tree
                        tree = ast.parse(f.read(), filename=file_path)

                        # Walk the tree to find all function definition nodes
                        for node in ast.walk(tree):
                            if isinstance(node, ast.FunctionDef):
                                # Exclude private/protected functions (e.g., _my_func)
                                if not node.name.startswith("_"):
                                    function_names.add(node.name)
                    except SyntaxError as e:
                        print(f"Could not parse {file_path}: {e}")

    return sorted(list(function_names))  # Return a sorted list


if __name__ == "__main__":
    all_funcs = find_all_functions()

    print("Found {} public functions.".format(len(all_funcs)))
    print("-" * 20)
    print("Copy this list into your pat2vec/__init__.py file:")
    print("-" * 20)

    # Format the output for direct copy-pasting into the __all__ list
    print("__all__ = [")
    for func in all_funcs:
        print(f'    "{func}",')
    print("]")
