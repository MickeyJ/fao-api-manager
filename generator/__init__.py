import re


ZIP_PATH = r"C:\Users\18057\Documents\Data\fao-test-zips"


def to_snake_case(text: str) -> str:
    """Convert text to snake_case"""
    # Remove parentheses and their contents
    text = re.sub(r"\([^)]*\)", "", text)

    # Handle camelCase and PascalCase
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    # Clean up and convert to lowercase
    result = s2.replace("-", "_").lower()
    # Remove multiple underscores
    result = re.sub("_+", "_", result)
    return result.strip("_")


def snake_to_pascal_case(snake_str: str) -> str:
    """Convert snake_case to PascalCase"""
    return "".join(word.capitalize() for word in snake_str.split("_"))
