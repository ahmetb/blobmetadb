using System;

namespace BlobMetaDb.Lib
{
	/// <summary>
	/// Contains saved metadata of a storage container.
	/// </summary>
	public class ContainerMeta
	{
		public string Name { get; private set; }

		public ContainerMeta (string name)
		{
			this.Name = name;
		}
	}

	/// <summary>
	/// Contains saved metadata of a blob.
	/// </summary>
	public class BlobMeta
	{
		public string Container { get; private set; }
		public string Name { get; private set; }

		public long? Size { get; private set; }

		public BlobMeta (string containerName, string blobName, long? size = null)
		{
			this.Container = containerName;
			this.Name = blobName;
			if (size.HasValue) {
				if (size < 0) {
					throw new ArgumentOutOfRangeException ("size");
				}
				this.Size = size;
			}
		}
	}
}

