using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BlobMetaDb.Lib
{
	/// <summary>
	/// An interface the metadata store is supposed to provide.
	/// </summary>
	public interface IBlobStorageMetadataStore
	{
		#region Container operations

		/// <summary>
		/// Saves the container record, if there exists a container with the same name,
		/// it should delete it and create new.
		/// </summary>
		Task CreateContainerAsync (string accountName, string name);

		/// <summary>
		/// If container record with given name DOES NOT exist, creates one. Otherwise, no-op.
		/// This method is called when a container is discovered by intercepting GET/HEAD requests.
		/// </summary>
		Task CreateContainerIfNotExistsAsync (string accountName, string name);

		/// <summary>
		/// Deletes the container record and blob records it contain.
		/// Ignores errors if container record does not exist in the first place.
		/// </summary>
		Task DeleteContainerAsync (string accountName, string name);

		/// <summary>
		/// Checks if the container record exists or not.
		/// </summary>
		Task<bool> ContainerExistsAsync (string accountName, string name);

		/// <summary>
		/// Enumerates the container records in the storage account.
		/// </summary>
		Task<IEnumerable<ContainerMeta>> ListContainersAsync (string accountName);

		#endregion

		#region Blob operations

		/// <summary>
		/// Enumerates the blobs records in the container.
		/// </summary>
		Task<IEnumerable<BlobMeta>> ListBlobsAsync (string accountName, string containerName, bool withMeta = false);

		/// <summary>
		/// Saves the blob record. If exists, overwrites with given properties.
		/// This should not modify/insert any container records.
		/// 
		/// There may be multiple calls to this method with same parameters
		/// when blob is discovered by intercepting GET/HEAD requests.
		/// </summary>
		Task PutBlobAsync (string accountName, string container, string blobName, long? size = null);

		/// <summary>
		/// Checks if the blob record exists in the given container.
		/// </summary>
		Task<bool> BlobExistsAsync (string accountName, string container, string blobName);

		/// <summary>
		/// Deletes the blob record if exists. Ignores errors if blob does not
		/// exists in the first place.
		/// </summary>
		Task DeleteBlobAsync (string accountName, string container, string blobName);

		#endregion
	}
}
