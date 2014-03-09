using System;

namespace BlobMetaDb.Lib
{
	/// <summary>
	/// Specifies which Blob Service operations can be used as a source to discover
	/// existance of blobs and containers.
	/// </summary>
	public enum DiscoveryMode
	{
		/// <summary>
		/// Discovers existance of containers and blobs from write operations like: 
		/// Create container, delete container, put blob, delete blob.
		/// </summary>
		OnlyWrites,
		/// <summary>
		/// Discovers existance of containers and blobs from all possible Blob Service calls like:
		/// Create container, get container or container properties or list blobs in a container (means container exists), get blob (means blob exists),
		/// get/put block list (means blob exists), lease blob ((means blob exists), lease container (means container exists),
		/// delete blob (means blob is now gone), delete container (means container is now gone).
		/// 
		/// This could put extra READ load if all containers & blobs are already known in metadata store.
		/// However if blobs are not all unknown, this is a good best effort to discover from existing calls.
		/// </summary>
		AllRequests,
		/// <summary>
		/// On top of DiscoverFromAllRequests, while saving blobs (even though they exist) queries metadata
		/// store and creates container record if does not exist (and then will update blob record).
		/// This will double the load on the metadata store for the requests that imply blobs exist (since it
		/// will first make sure the container exists.)
		/// </summary>
		AllRequestsEnsureSchema
	}
}
