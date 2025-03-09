class DeleteError:
    """Delete error information. This class is copied (and truncated) from Minio library"""

    def __init__(self, code, message, name):
        self._code = code
        self._message = message
        self._name = name

    @property
    def code(self):
        """Get error code."""
        return self._code

    @property
    def message(self):
        """Get error message."""
        return self._message

    @property
    def name(self):
        """Get name."""
        return self._name
