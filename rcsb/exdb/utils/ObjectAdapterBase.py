##
# File:  ObjectAdapterBase.py
# Date:  17-Oct-2019
#
##


class ObjectAdapterBase(object):
    def __init(self, *args, **kwargs):
        pass

    def filter(self, obj, **kwargs):
        """Operates on the input object and returns the transformed result.

        Args:
            obj (object): input object/document

        Returns:

            bool, object: filter status and transformed input object/document
        """
        raise NotImplementedError
