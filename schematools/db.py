from sqlalchemy import inspect


def fetch_table_names(engine):
    """ Fetches all tablenames, to be used in other commands
    """
    insp = inspect(engine)
    return insp.get_table_names()
