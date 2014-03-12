blobmetadb
==========

An isometric metadata provider shim for Azure Blob Storage. Okay that's a shitty description. **blobmetadb** keeps track of your blobs and containers on the cloud and can provide info about it them using an offline database, like Redis.

Tasks like **listing blobs**, **listing containers**, **checking if blob exists** are <u>lightning-fast</u> compared to API requests to Azure Storage Service.

It plugs in very easily, with a little code change and a NuGet package to install and you're done.

##### Example List Blobs: Azure

In a container that has 20,000 blobs:

```csharp
var account = new CloudStorageAccount (StorageCredentials ("ACCOUNT", "KEY"), true);
var blobs = client.GetContainerReference ("containerName").ListBlobs();
>>> Took 15.701 seconds.
```

##### Example List Blobs: BlobMetaDb

```csharp
var blobTracker = new BlobMetaTracker (DiscoveryMode.AllRequests, new RedisBlobMetadataStore (), account);
var blobs = blobTracker.ListBlobsAsync("containerName").Result;
>>> Took 0.0491 seconds.
```

That's **300x faster** and free (you don't make calls to Storage API).


### Works with any key-value store

**blobmetadb** smartly works by listening Storage Service traffic of your application and understands what you store on the cloud and tell you information about blobs and containers you have, sizes of them very quickly.

We provide **Redis support out of the box** ('cause it's really cool.) Heck, you can even purchase a Redis instace from [AWS Elasticache](http://aws.amazon.com/elasticache/) (or host it yourself). You can even use SQLite, SQL Server, MySQL or whatever as long as you write implementation of IBlobStorageMetadataStore interface.


### Easy to plug in

You just make use of an optional `operationContext` parameter that exists on all [official Azure Storage Client](https://github.com/WindowsAzure/azure-sdk-for-net) operations. You just add the following to your code:

```
OperationContext context = new OperationContext();
context.ResponseReceived += blobTracker.UseReceivedResponseAsync;
```

and from then on

<table>
	<tr>
	<th>Replace</th>
	<th>…To:</th>
	</tr>
	<tr>
		<td>
			`client.CreateContainer(name)`
		</td>
		<td>
			`client.CreateContainer(name, operationContext: ctx)`
		</td>
	</tr>
	<tr>
		<td>
			`blob.UploadFromFile(path)`
		</td>
		<td>
			`blob.UploadFromFile(path, operationContext: ctx)`
		</td>
	</tr>
	<tr>
		<td>
			`blob.Delete()`
		</td>
		<td>
			`blob.Delete(operationContext: ctx)`
		</td>
	</tr>
	<tr>
		<td>
			`container.DeleteIfExists()`
		</td>
		<td>
			`container.DeleteIfExists(operationContext: ctx)`
		</td>
	</tr>
</table>

and so on… I bet you got the idea.

> **YMMV:** Your blobs will be tracked only if all your blob requests are using operationContext. Previously created blobs and containers can be imported. Read on.

### It's smart

It uses various hints from HTTP responses sent from Azure Storage Service to interpret blobs & containers are created or deleted, and even sizes of blobs. Just plug in, like shown just above.

### Force synchronization & initial import

If you're already using Azure Storage Service for a while, when you initialize `BlobMetaTracker` instance just call `blobTracker.ImportAllContainersAsync()` once and it will import metadata (existance, size etc.) of blobs and containers to your local database.

It does not intercept calls you explicitly make to blob storage service (e.g. `container.ListBlobs()` or `blob.Exists()`). You need to make calls to BlobMetaTracker class directly (e.g. `blobTracker.ListBlobs(container)` or `blobTracker.BlobExists(container, blob)`).

### Wiki

More info on [wiki](https://github.com/ahmetalpbalkan/blobmetadb/wiki/_pages):

* [How it works](https://github.com/ahmetalpbalkan/blobmetadb/wiki/How-it-works)
* [Discover Modes](https://github.com/ahmetalpbalkan/blobmetadb/wiki/Discovery-Modes)
* [List of tracked Blob Service Requests](https://github.com/ahmetalpbalkan/blobmetadb/wiki/Table-of-interpreted-Blob-Service-Requests)

### Few cool facts

* It is a **fully asynchronous** library.
* It's distributed on **NuGet**.
* It will not keep in sync with your storage account by itself, you need to use this everywhere in your code to keep local database up to date.
* It is inspired by Nathan Cahill's [mimicdb](https://github.com/nathancahill/mimicdb).
* It is developed in Seattle, on a MacBook using Mono.

### FAQ

**Q: Is it a cache for blobs?**<br/>
A: No, it is not. It can just tell you if blob or container exists and can list you the containers in your account and blobs in a container.

**Q: Is it supported or developed by Microsoft?**<br/>
A: No, it is not. This is merely a side project and has nothing to do with Microsoft Corporation. It is not anything official. See [license](LICENSE).
