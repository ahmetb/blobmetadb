using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace BlobMetaDb.Lib.Test
{

	/// <summary>
	/// This is a mock implementation to capture requests arrive to
	/// <see cref="BlobMetaDb.Lib.IBlobMetaDataStore"/>. Internally uses
	/// in-memory metadata store 
	/// </summary>
	public class InMemoryBlobMetadataStore : IBlobStorageMetadataStore
	{
		public MetadataStoreOperation LastOperation { get; private set; }

		internal IDictionary<string, ISet<string>> _containers;
		internal IDictionary<string, BlobMeta> _blobs;

		public InMemoryBlobMetadataStore ()
		{
			_containers = new Dictionary<string, ISet<string>> ();
			_blobs = new Dictionary<string, BlobMeta> ();
		}

		internal string NameForBlob(string account, string container, string blob){
			return string.Format ("{0}:{1}:{2}", account, container, blob);
		}

		internal string NameForContainer(string account, string container){
			return string.Format ("{0}:{1}", account, container);
		}

		public async Task CreateContainerAsync (string accountName, string name){
			await Task.Delay (0);
			LastOperation |= MetadataStoreOperation.CreateContainer;
		}

		public async Task CreateContainerIfNotExistsAsync (string accountName, string name){
			LastOperation |= MetadataStoreOperation.CreateContainerIfNotExists;
			if (!_containers.ContainsKey (NameForContainer(accountName, name))) {
				await CreateContainerAsync (accountName, name);
			}
		}

		public async Task DeleteContainerAsync (string accountName, string name){
			await Task.Delay (0);
			_containers.Remove (NameForContainer(accountName, name));
			LastOperation |= MetadataStoreOperation.DeleteContainer;
		}

		public async Task<bool> ContainerExistsAsync (string accountName, string name){
			await Task.Delay (0);
			return _containers.ContainsKey(NameForContainer(accountName, name));
		}
	
		public async Task<IEnumerable<ContainerMeta>> ListContainersAsync(string accountName){
			await Task.Delay (0);
			return from cn in _containers.Keys
				select new ContainerMeta (cn.Split(':').LastOrDefault());
		}

		public async Task<IEnumerable<BlobMeta>> ListBlobsAsync(string accountName, string name, bool withMeta){
			await Task.Delay (0);
			if (!ContainerExistsAsync (accountName, name).Result) {
				return Enumerable.Empty<BlobMeta> ();
			} else {
				return (from bn in _containers [NameForContainer(accountName, name)]
				        select _blobs [bn]);
			}
		}

		public async Task PutBlobAsync(string accountName, string container, string blobName, long? size = null){
			await Task.Delay (0);
			LastOperation |= MetadataStoreOperation.PutBlob;

			_blobs[NameForBlob(accountName, container, blobName)] = new BlobMeta(container, blobName, size);
		}

		public async Task<bool> BlobExistsAsync (string accountName, string container, string blobName){
			await Task.Delay (0);
			LastOperation |= MetadataStoreOperation.CreateContainer;
			return _blobs.ContainsKey(NameForBlob(accountName, container, blobName));
		}

		public async Task DeleteBlobAsync(string accountName, string container, string blobName){
			await Task.Delay (0);
			LastOperation |= MetadataStoreOperation.DeleteBlob;

			if (_blobs.Remove (NameForBlob (accountName, container, blobName))) {
				_containers.Remove (blobName);
			}
		}
	}

	[Flags]
	public enum MetadataStoreOperation {
		Undefined = 0,
		CreateContainer = 1<<1,
		CreateContainerIfNotExists = 1<<2 | CreateContainer, // could call CreateContainer internally.
		DeleteContainer = 1<<3,
		PutBlob = 1<<4,
		DeleteBlob = 1<<5,
	}
}

