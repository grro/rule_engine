
def when(target: str):
    """
    Examples:
        .. code-block::
            @when("Time cron 55 55 5 * * ?")
            @when("Property energy#pv changed")
            @when("Rule loaded")
    Args:
        target (string): the trigger expression
    """

    def decorated_method(function):
        return function
    return decorated_method
