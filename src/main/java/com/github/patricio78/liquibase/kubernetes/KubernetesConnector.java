package com.github.patricio78.liquibase.kubernetes;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.credentials.TokenFileAuthentication;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import static io.kubernetes.client.util.Config.SERVICEACCOUNT_CA_PATH;
import static io.kubernetes.client.util.Config.SERVICEACCOUNT_TOKEN_PATH;

public class KubernetesConnector {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesConnector.class);
    public static final String POD_PHASE_PENDING = "Pending";
    public static final String POD_PHASE_RUNNING = "Running";
    public static final int HTTP_STATUS_NOT_FOUND = 404;
    private final boolean connected;
    private final String podName;
    private final String podNamespace;

    private static final class InstanceHolder {
        private static final KubernetesConnector INSTANCE = new KubernetesConnector();
    }

    public static KubernetesConnector getInstance() {
        return InstanceHolder.INSTANCE;
    }

    private KubernetesConnector() {
        podName = System.getenv().get("POD_NAME");
        podNamespace = System.getenv().get("POD_NAMESPACE");
        if(StringUtils.isNotBlank(podName)&& StringUtils.isNotBlank(podNamespace)){
            connected = connect();
        } else {
            connected = false;
            LOG.warn("POD_NAME or POD_NAMESPACE is not configured, Liquibase - Kubernetes integration disabled");
        }
    }

    private boolean connect() {
        try {
            LOG.info("Create client with from cluster configuration");

            final String serviceAccountCaPath = System.getenv().getOrDefault("SERVICEACCOUNT_CA_PATH", SERVICEACCOUNT_CA_PATH);
            final String serviceAccountTokenPath = System.getenv().getOrDefault("SERVICEACCOUNT_TOKEN_PATH", SERVICEACCOUNT_TOKEN_PATH);

            LOG.info("Service account CA path: {}, Service account token path: {}", serviceAccountCaPath, serviceAccountTokenPath);

            final ApiClient client = ClientBuilder.cluster()
                    .setCertificateAuthority(Files.readAllBytes(Paths.get(serviceAccountCaPath)))
                    .setAuthentication(new TokenFileAuthentication(serviceAccountTokenPath))
                    .build();

            Configuration.setDefaultApiClient(client);

            LOG.info("BasePath: {}", client.getBasePath());

            if (LOG.isTraceEnabled()) {
                LOG.trace("Authentication: {}", client.getAuthentications().entrySet().stream().map(entry -> entry.getKey() + ":" + entry.getValue()).collect(Collectors.joining(", ")));
            }

            CoreV1Api api = new CoreV1Api();
            LOG.info("Reading pod status, Pod name: {} Pod namespace: {}", podName, podNamespace);
            V1Pod pod = api.readNamespacedPodStatus(podName, podNamespace, "true");
            if (pod == null || pod.getStatus() == null) {
                return false;
            }
            String podPhase = pod.getStatus().getPhase();
            LOG.info("Pod phase: {}", podPhase);
            LOG.info("Connected to Kubernetes using fromCluster configuration");
            return true;
        } catch (IOException | ApiException e) {
            LOG.error("Connection fail to Kubernetes cluster using fromCluster configuration");
            LOG.error("Pod status read error", e);
            return false;
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public String getPodName() {
        return podName;
    }

    public String getPodNamespace() {
        return podNamespace;
    }

    public boolean isCurrentPod(String podNamespace, String podName) {
        if (podNamespace == null || podName == null) {
            return false;
        }
        return podNamespace.equals(this.podNamespace) && podName.equals(this.podName);
    }

    public boolean isPodActive(String podNamespace, String podName) {
        try {
            CoreV1Api api = new CoreV1Api();
            LOG.info("Reading pod status, Pod name: {} Pod namespace: {}", podName, podNamespace);
            V1Pod pod;

            pod = api.readNamespacedPodStatus(podName, podNamespace, "true");
            if (pod == null || pod.getStatus() == null) {
                return false;
            }
            String podPhase = pod.getStatus().getPhase();

            if(POD_PHASE_PENDING.equals(podPhase) || POD_PHASE_RUNNING.equals(podPhase)){
                LOG.info("Pod is active, phase: {}",podPhase);
                return true;
            } else {
                LOG.info("Pod is inactive, phase: {}", podPhase);
                return false;
            }
        } catch (ApiException e) {
            if(e.getCode() == HTTP_STATUS_NOT_FOUND){
                LOG.error("Can't find pod");
                return false;
            }
            LOG.error("Can't read Pod status: {}:{}", podNamespace, podName);
            LOG.error("Pod status read error", e);
            return false;
        }
    }
}
