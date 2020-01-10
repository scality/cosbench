package com.intel.cosbench.api.azureblob;

import com.intel.cosbench.api.storage.StorageAPI;
import com.intel.cosbench.api.storage.StorageAPIFactory;

public class AzureBlobStorageFactory implements StorageAPIFactory {

	@Override
	public String getStorageName() {
		return "azureblob";
	}

	@Override
	public StorageAPI getStorageAPI() {
		return new AzureBlobStorage();
	}

}
