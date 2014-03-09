using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BlobMetaDb.Lib
{
	public class BlobMetaTracker
	{
		private IBlobStorageMetadataStore store { get; set; }

		private CloudStorageAccount account { get; set; }

		private CloudBlobClient client { get; set; }

		private string accountName { get; set; }

		private DiscoveryMode discoveryMode { get; set; }
		// TODO hardcoded values for now
		internal const int containerListImportBatchSize = 100;
		internal const int blobListImportBatchSize = 2000;
		internal const int containerParallelBatchSize = 3;
		internal const int blobParallelBatchSize = 30;

		public BlobMetaTracker (DiscoveryMode discoveryMode, IBlobStorageMetadataStore store, CloudStorageAccount account, bool useHttps = true)
		{
			if (store == null)
				throw new ArgumentNullException ("store");
			if (account == null)
				throw new ArgumentNullException ("account");
			this.discoveryMode = discoveryMode;
			this.store = store;
			this.account = account;
			this.accountName = account.Credentials.AccountName;
			this.client = account.CreateCloudBlobClient ();
		}

		#region Event Handlers

		/// <summary>
		/// Event handler for OperationContext.ResponseReceived event. Register this method to this event
		/// to start tracking blob service requests.
		/// 
		/// Do not call directly.
		/// </summary>
		public async void UseReceivedResponseAsync (object sender, RequestEventArgs e)
		{
			// This method should be thread-safe and idempotent since it can be called with
			// multiple events concurrently.
			// This method also should NOT throw ANY exceptions (this will bring the whole process down
			// since "async void" methods run in the background.)
			try {
				if (e.Response != null && ((int)e.Response.StatusCode / 100 == 2)) { // Only successful requests
					bool handled = await new RequestRouter (discoveryMode, store, e.Request, e.Response).RouteAsync ();
					Console.WriteLine ("---  Handled = " + handled.ToString ().ToUpperInvariant ()); 
				}
			} catch {
				//TODO provide feedback mechanism to library user.
			}
		}

		#endregion

		#region Data Provider Methods

		public async Task<bool> ContainerExistsAsync (string containerName)
		{
			return await store.ContainerExistsAsync (accountName, containerName);
		}

		public Task<IEnumerable<ContainerMeta>> ListContainersAsync ()
		{
			return store.ListContainersAsync (accountName);
		}

		public Task<IEnumerable<BlobMeta>> ListBlobsAsync (string containerName, bool withMeta = false)
		{
			return store.ListBlobsAsync (accountName, containerName, withMeta);
		}

		public Task<bool> BlobExistsAsync (string containerName, string blobName)
		{
			return store.BlobExistsAsync (accountName, containerName, blobName);
		}

		#endregion

		#region Import Methods

		public async Task ImportAllContainersAsync (CancellationToken token = default(CancellationToken))
		{
			BlobContinuationToken continuationToken = null;
			var options = new BlobRequestOptions ();
			do {
				token.ThrowIfCancellationRequested ();

				var resp = await client.ListContainersSegmentedAsync (null, ContainerListingDetails.None, containerListImportBatchSize, continuationToken, options, null);
				continuationToken = resp.ContinuationToken;
				var containers = resp.Results;
				foreach (var container in containers) {
					token.ThrowIfCancellationRequested ();
					await ImportContainerAsync (container, token);
				}
			} while (continuationToken != null);
		}

		internal async Task ImportContainerAsync (CloudBlobContainer container, CancellationToken token = default(CancellationToken))
		{
			await store.CreateContainerIfNotExistsAsync (accountName, container.Name);

			BlobContinuationToken continuationToken = null;
			var options = new BlobRequestOptions ();
			do {
				token.ThrowIfCancellationRequested ();
				var resp = await container.ListBlobsSegmentedAsync (null, true, BlobListingDetails.None, blobListImportBatchSize, continuationToken, options, null);
				continuationToken = resp.ContinuationToken;
				var blobs = resp.Results;
				foreach (var blob in blobs) {
					token.ThrowIfCancellationRequested ();
					var resolver = new UrlResolver (blob.Uri);
					var bb = (ICloudBlob)blob;
					long? size = bb.Properties != null ? bb.Properties.Length : (long?)null;
					Console.WriteLine ("Recorded size " + size + " bytes.");

					if (size == null) {
						// HIT MA BREAKPOINT BABE
					}
					await store.PutBlobAsync (accountName, resolver.GetContainerName (), resolver.GetBlobName (), size);
				}
			} while (continuationToken != null);
		}

		public async Task ImportContainerAsync (string containerName, CancellationToken token = default(CancellationToken))
		{
			await ImportContainerAsync (client.GetContainerReference (containerName), token);
		}

		#endregion
	}
}

