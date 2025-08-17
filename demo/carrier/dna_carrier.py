from abc import ABC, abstractmethod


class DNACarrier(ABC):
    """
    Interface for moving data from a source to a destination.

    Implementations can define how 'source' and 'dest' are interpreted
    (local paths, URLs, object stores, etc.).
    """

    @abstractmethod
    def move_folder(
        self,
        source_folder: str,
        dest_folder: str,
        recursive: bool = True,
        overwrite: bool = False,
        # TODO: Add filter interface if necessary
    ) -> None:
        """
        Move data from 'source' to 'dest'.

        Parameters
        ----------
        source_folder : str
            The source location.
        dest_folder : str
            The destination location.
        recursive : bool
            Controlling if the look-up will be recursive or not
        overwrite : bool
            If False (default), raise FileExistsError when dest exists.
            If True, replace the destination if it exists.
        """
        raise NotImplementedError()
