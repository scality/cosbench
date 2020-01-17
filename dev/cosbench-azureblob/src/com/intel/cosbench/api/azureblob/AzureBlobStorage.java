package com.intel.cosbench.api.azureblob;

import static com.intel.cosbench.client.azureblob.AzureBlobConstants.ACCOUNT_NAME_KEY;
import static com.intel.cosbench.client.azureblob.AzureBlobConstants.ACCOUNT_NAME_DEFAULT;
import static com.intel.cosbench.client.azureblob.AzureBlobConstants.ACCOUNT_KEY_KEY;
import static com.intel.cosbench.client.azureblob.AzureBlobConstants.ACCOUNT_KEY_DEFAULT;
import static com.intel.cosbench.client.azureblob.AzureBlobConstants.SAS_TOKEN_KEY;
import static com.intel.cosbench.client.azureblob.AzureBlobConstants.SAS_TOKEN_DEFAULT;
import static com.intel.cosbench.client.azureblob.AzureBlobConstants.ENDPOINT_KEY;
import static com.intel.cosbench.client.azureblob.AzureBlobConstants.ENDPOINT_DEFAULT;
import static com.intel.cosbench.client.azureblob.AzureBlobConstants.USE_HTTPS_KEY;
import static com.intel.cosbench.client.azureblob.AzureBlobConstants.USE_HTTPS_DEFAULT;
import static com.intel.cosbench.client.azureblob.AzureBlobConstants.ENDPOINT_SUFFIX_KEY;
import static com.intel.cosbench.client.azureblob.AzureBlobConstants.ENDPOINT_SUFFIX_DEFAULT;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.UUID;
import java.io.InputStream;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.BlobContainerPermissions;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

import com.intel.cosbench.api.context.AuthContext;
import com.intel.cosbench.api.context.Context;
import com.intel.cosbench.api.storage.NoneStorage;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.Logger;

public class AzureBlobStorage extends NoneStorage {

	private CloudBlobClient cloudBlobClient;

	@Override
	public void init(Config config, Logger logger) {
				super.init(config, logger);

                String accountName = config.get(ACCOUNT_NAME_KEY, ACCOUNT_NAME_DEFAULT);
                String accountKey = config.get(ACCOUNT_KEY_KEY, ACCOUNT_KEY_DEFAULT);

                String sasToken = config.get(SAS_TOKEN_KEY, SAS_TOKEN_DEFAULT);
                
                String useHttps = config.get(USE_HTTPS_KEY, USE_HTTPS_DEFAULT);
                
                String storageConnectionString = "";
                
                if (useHttps != "true") {
                	storageConnectionString +=
                			"DefaultEndpointsProtocol=http;";
                } else {
                	storageConnectionString +=
                    		"DefaultEndpointsProtocol=https;";
                }
              
                if (sasToken != "") {
                	storageConnectionString +=
                			"SharedAccessSignature=" + sasToken + ";";
                } else {
                	storageConnectionString +=
                			"AccountName=" + accountName + ";" +
                    		"AccountKey=" + accountKey + ";";
                }
                        
                String endpoint = config.get(ENDPOINT_KEY, ENDPOINT_DEFAULT);
                if (endpoint != "") {
                	storageConnectionString +=
                			"BlobEndpoint=" + endpoint + ";";
                } // else {
                    // not necessary
                    // String _endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);
                // }
                
                String endpointSuffix = config.get(ENDPOINT_SUFFIX_KEY, ENDPOINT_SUFFIX_DEFAULT);
                if (endpointSuffix != "") {
                	storageConnectionString +=
                			"EndpointSuffix=" + endpointSuffix + ";";
                }
                
                logger.info("connectionString" + storageConnectionString);

                CloudStorageAccount account;
                try {
                	account = CloudStorageAccount.parse(storageConnectionString);
                } catch (InvalidKeyException e) {
        			throw new com.intel.cosbench.api.storage.StorageException("error message:" + e.getMessage(), e);  	
                } catch (URISyntaxException e) {
        			throw new com.intel.cosbench.api.storage.StorageException("error message:" + e.getMessage(), e);  	
                }
               cloudBlobClient = account.createCloudBlobClient();

               logger.debug("AzureBlob client has been initialized");
	}

	@Override
	public void setAuthContext(AuthContext info) {
		super.setAuthContext(info);
	}

	@Override
	public void dispose() {
		super.dispose();
		cloudBlobClient = null;
	}

	@Override
	public Context getParms() {
		return super.getParms();
	}

	@Override
	public InputStream getObject(String container, String object, Config config) {
		super.getObject(container, object, config);
		InputStream stream;
		try {
			CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference(container);
			CloudBlockBlob cloudBlockBlob = cloudBlobContainer.getBlockBlobReference(object);
			stream = cloudBlockBlob.openInputStream();
		} catch (com.microsoft.azure.storage.StorageException azureBlobExce) {
			throw new com.intel.cosbench.api.storage.StorageException("error message:" + azureBlobExce.getMessage(), azureBlobExce);
		} catch (Exception e) {
			throw new com.intel.cosbench.api.storage.StorageException(e);
		}
		return stream;
	}

	@Override
	public void createContainer(String container, Config config) {
		super.createContainer(container, config);
		try {
			CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference(container);
			if (!cloudBlobContainer.exists()) {
				cloudBlobContainer.create();
			}
		} catch (com.microsoft.azure.storage.StorageException azureBlobExce) {
			throw new com.intel.cosbench.api.storage.StorageException(azureBlobExce.getMessage(), azureBlobExce);
		} catch (Exception e) {
			throw new com.intel.cosbench.api.storage.StorageException(e);
		}
	}

	@Override
	public void createObject(String container, String object, InputStream data, long length, Config config) {
		super.createObject(container, object, data, length, config);
		try {
			CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference(container);
			CloudBlockBlob cloudBlockBlob = cloudBlobContainer.getBlockBlobReference(object);
			cloudBlockBlob.upload(data, length);
		} catch (com.microsoft.azure.storage.StorageException azureBlobExce) {
			throw new com.intel.cosbench.api.storage.StorageException(azureBlobExce.getMessage(), azureBlobExce);
		} catch (Exception e) {
			throw new com.intel.cosbench.api.storage.StorageException(e);
		}
	}

	@Override
	public void deleteContainer(String container, Config config) {
		super.deleteContainer(container, config);
		try {
			CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference(container);

			if (cloudBlobContainer.exists()) {
				cloudBlobContainer.delete();
			}
		} catch (com.microsoft.azure.storage.StorageException azureBlobExce) {
			throw new com.intel.cosbench.api.storage.StorageException(azureBlobExce.getMessage(), azureBlobExce);
		} catch (Exception e) {
			throw new com.intel.cosbench.api.storage.StorageException(e);
		}
	}

	@Override
	public void deleteObject(String container, String object, Config config) {
		super.deleteObject(container, object, config);

		try {
			CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference(container);
			CloudBlockBlob cloudBlockBlob = cloudBlobContainer.getBlockBlobReference(object);
			cloudBlockBlob.delete();
		} catch (com.microsoft.azure.storage.StorageException azureBlobExce) {
			throw new com.intel.cosbench.api.storage.StorageException(azureBlobExce.getMessage(), azureBlobExce);
		} catch (Exception e) {
			throw new com.intel.cosbench.api.storage.StorageException(e);
		}
	}
}
