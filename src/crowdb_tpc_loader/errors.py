"""Errors with stable, script-friendly exit codes."""


class LoaderError(Exception):
    exit_code = 1


class ArgumentError(LoaderError):
    exit_code = 2


class GenerationError(LoaderError):
    exit_code = 3


class ValidationError(GenerationError):
    pass


class LoadError(LoaderError):
    exit_code = 4


class CompatibilityError(LoadError):
    pass


class ExistingTablesError(LoadError):
    pass


class CommitUncertainError(LoadError):
    pass


class ResourceError(LoaderError):
    exit_code = 5
