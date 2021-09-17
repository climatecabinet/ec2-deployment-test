import re


def find_first_from_regex(pattern, search_list):
    """
    Finds the first element in a list that matches a regex expression
    """
    if not isinstance(pattern, str):
        raise TypeError(f"'{pattern}' is not a valid regex pattern.")

    # if there are any elements in the search list that aren't strings
    if [elem for elem in search_list if not isinstance(elem, str)]:
        raise ValueError("All elements of the provided list must be strings.")

    regex = re.compile(pattern)
    found = list(filter(regex.match, search_list))

    if not found:
        raise ValueError(f"Pattern '{pattern}' does not match "
                         f"any element of the provided list.")

    return found[0]
