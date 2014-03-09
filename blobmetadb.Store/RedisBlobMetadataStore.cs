using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using BlobMetaDb.Lib;
using BookSleeve;

namespace BlobMetaDb.Store.Redis
{
	public class RedisBlobMetadataStore : IBlobStorageMetadataStore
	{
		private readonly RedisConnection redis;
		private readonly int db;
		internal readonly string GlobalKeyPrefix;

		public RedisBlobMetadataStore (int db = 0, string keyPrefix = null) : this (new RedisConnection ("localhost", allowAdmin: true), db, keyPrefix)
		{
		}

		public RedisBlobMetadataStore (RedisConnection client, int db = 0, string keyPrefix = null)
		{
			client.Open ().Wait (); // Have to wait for connection to be established.
			this.db = db;
			this.redis = client;
			this.GlobalKeyPrefix = keyPrefix;
		}

		#region Key handling

		private const string kContainer = "container";
		private const string kBlob = "blob";
		private const string kBlobs = "blobs";
		private const string kContainers = "containers";
		private const string khBlobCreated = "created", hkContainerCreated = khBlobCreated;
		private const string hkBlobSize = "size";
		internal const char KeyDelimiter = ':';
		internal const char ContainerAndBlobDelimiter = '/';

		internal string PrefixWithGlobal (string key)
		{
			if (string.IsNullOrEmpty (GlobalKeyPrefix)) {
				return key;
			}
			return GlobalKeyPrefix + KeyDelimiter + key;
		}

		internal string PrefixWithAccount (string account, string key)
		{
			return PrefixWithGlobal (account + KeyDelimiter + key);
		}

		internal string KeyForContainers (string accountName)
		{
			return PrefixWithAccount (accountName, kContainers);
		}

		internal string KeyForContainer (string accountName, string containerName)
		{
			return PrefixWithAccount (accountName, kContainer + KeyDelimiter + containerName);
		}

		internal string KeyForContainerBlobs (string accountName, string containerName)
		{
			return PrefixWithAccount (accountName, kBlobs + KeyDelimiter + containerName);
		}

		internal string KeyForBlob (string accountName, string containerName, string blobName)
		{
			return PrefixWithAccount (accountName, kBlob + KeyDelimiter + (containerName + ContainerAndBlobDelimiter + blobName));
		}

		internal static string UtcNowIso8601 {
			get {
				return DateTime.UtcNow.ToString ("s");
			}
		}

		#endregion

		public async Task CreateContainerAsync (string accountName, string name)
		{
			// Create brand new container
			// 1. Delete container with its blobs (even though it doesn't exist)
			// 2. Add container record to 'containers' set
			// 3. Create container

			await DeleteContainerAsync (accountName, name);

			// SADD containers cnt

			await redis.Sets.Add (db, KeyForContainers (accountName), name);

			// HSET container:cnt created DATE
			string cntKey = KeyForContainer (accountName, name);
			await redis.Hashes.Set (db, cntKey, hkContainerCreated, UtcNowIso8601);
		}

		public async Task CreateContainerIfNotExistsAsync (string accountName, string name)
		{
			// EXISTS container:cnt
			if (!await redis.Keys.Exists (db, KeyForContainer (accountName, name))) {
				await CreateContainerAsync (accountName, name);
			}
		}

		public async Task DeleteContainerAsync (string accountName, string cname)
		{
			// 1. Delete each 'blob:' record it contains
			// 2. Delete 'blobs:' list of container
			// 3. Delete 'container:' record itself
			// 3. Delete from 'containers' set

			// SMEMBERS blobs:cnt
			var blobNames = await redis.Sets.GetAllString (db, KeyForContainerBlobs (accountName, cname));
			var blobKeys = (from bn in blobNames
			                select KeyForBlob (accountName, cname, bn)).ToArray ();
			
			if (blobKeys.Length > 0) {
				// DEL blob:cnt/a.txt blob:cnt/b.txt blob:cnt/c.txt...
				await redis.Keys.Remove (db, blobKeys);
			}
			// DEL blobs:cnt
			await redis.Keys.Remove (db, KeyForContainerBlobs (accountName, cname));
			// DEL container:cnt
			await redis.Keys.Remove (db, KeyForContainer (accountName, cname));
			// DEL containers cnt
			await redis.Sets.Remove (db, KeyForContainers (accountName), cname);
			
		}

		public async Task<bool> ContainerExistsAsync (string accountName, string name)
		{
			// EXISTS container:cnt
			return await redis.Keys.Exists (db, KeyForContainer (accountName, name));
		}

		public async Task<IEnumerable<ContainerMeta>> ListContainersAsync (string accountName)
		{
			var containerNames = await redis.Sets.GetAllString (db, KeyForContainers (accountName));
			return (from n in containerNames
			        select new ContainerMeta (n));
		}

		public async Task<IEnumerable<BlobMeta>> ListBlobsAsync (string accountName, string containerName, bool withMeta)
		{
			var blobNames = await redis.Sets.GetAllString (db, KeyForContainerBlobs (accountName, containerName));
			IList<BlobMeta> list = new List<BlobMeta> ();
			foreach (var blobName in blobNames) {

				long? size = null;

				if (withMeta) {
					// Foreach blob, 'try' to get its record so that we can get metadata e.g. size

					// HGET blob:cnt/blob.txt size
					size = await redis.Hashes.GetInt64 (db, KeyForBlob (accountName, containerName, blobName), hkBlobSize);
				}
				list.Add (new BlobMeta (containerName, blobName, size));
			}
			return list;
		}

		public async Task PutBlobAsync (string accountName, string containerName, string blobName, long? size = null)
		{
			var blobKey = KeyForBlob (accountName, containerName, blobName);
			// HSET blob:cnt/blob.txt created DATE
			await redis.Hashes.Set (db, blobKey, khBlobCreated, UtcNowIso8601);

			if (size.HasValue) {
				// HSET blob:cnt/blob.txt size 1234
				await redis.Hashes.Set (db, blobKey, hkBlobSize, size.ToString ());
			}

			// SADD blobs:cnt blob.txt
			await redis.Sets.Add (db, KeyForContainerBlobs (accountName, containerName), blobName);
		}

		public async Task<bool> BlobExistsAsync (string accountName, string containerName, string blobName)
		{
			// EXISTS blob:cnt/blob.txt
			return await redis.Keys.Exists (db, KeyForBlob (accountName, containerName, blobName));
				
		}

		public async Task DeleteBlobAsync (string accountName, string containerName, string blobName)
		{
			// DEL blob:cnt/blob.txt
			await redis.Keys.Remove (db, KeyForBlob (accountName, containerName, blobName));

			// SREM blobs:cnt blob.txt
			await redis.Sets.Remove (db, KeyForContainerBlobs (accountName, containerName), blobName);
		}
	}
}

