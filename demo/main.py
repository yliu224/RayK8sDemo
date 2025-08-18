from injector import Injector

from demo.file_system.azure_storage_file_system import AzureStorageFileSystem, Container
from demo.file_system.dna_nexus_file_system import DNANexusFileSystem, Project, Token
from demo.modules.file_system_module import FileSystemModule


def main() -> None:
    injector = Injector(
        [
            FileSystemModule(
                connection_str="",
                project=Project(""),
                token=Token(""),
                container=Container(""),
            )
        ]
    )
    dna_nexus = injector.get(DNANexusFileSystem)
    az_storage = injector.get(AzureStorageFileSystem)

    files = dna_nexus.list_folder("/resources")
    print(f"Listed {len(files)} files")
    dna_nexus.download_files(files, "/tmp/data/resources")
    print(f"Download {len(files)} files to /tmp/data/resources")
    az_storage.upload_files("/tmp/data/resources", "test/")
    print(f"Upload {len(files)} files to test/")


if __name__ == "__main__":
    main()
