package com.google.cloud.bigquery.connector.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

/** Utility class to fetch secrets from AWS Secrets Manager */
public class SecretManagerUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SecretManagerUtil.class);

  private static final Region DEFAULT_REGION = Region.US_EAST_2;
  private static SecretsManagerClient secretsClient;

  /**
   * Get a secret from AWS Secrets Manager
   *
   * @param secretName The name of the secret to fetch
   * @return The secret value or null if it could not be retrieved
   */
  public static String getSecret(String secretName) {
    try {
      if (secretsClient == null) {
        initSecretsClient();
      }

      GetSecretValueRequest valueRequest =
          GetSecretValueRequest.builder().secretId(secretName).build();

      GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
      return valueResponse.secretString();
    } catch (SecretsManagerException e) {
      LOG.error("Error retrieving secret from AWS Secrets Manager: {}", secretName, e);
      return null;
    }
  }

  /** Initialize the AWS Secrets Manager client */
  private static synchronized void initSecretsClient() {
    if (secretsClient == null) {
      LOG.info("Initializing AWS Secrets Manager client");
      secretsClient = SecretsManagerClient.builder().region(DEFAULT_REGION).build();
    }
  }

  /** Get the Datadog API key from AWS Secrets Manager */
  public static String getDatadogApiKey() {
    return getSecret("datadog/sundial/api_key");
  }

  /** Get the Datadog App key from AWS Secrets Manager */
  public static String getDatadogAppKey() {
    return getSecret("datadog/sundial/app_key");
  }
}
