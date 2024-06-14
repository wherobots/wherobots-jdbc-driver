package com.wherobots.db;

public enum AppStatus {
    PENDING,
    PREPARING,
    PREPARE_FAILED,
    REQUESTED,
    DEPLOYING,
    DEPLOY_FAILED,
    DEPLOYED,
    INITIALIZING,
    INIT_FAILED,
    READY,
    DESTROY_REQUESTED,
    DESTROYING,
    DESTROY_FAILED,
    DESTROYED;

    public boolean isStarting() {
        return this == PENDING
                || this == PREPARING
                || this == REQUESTED
                || this == DEPLOYING
                || this == DEPLOYED
                || this == INITIALIZING;
    }

    public boolean isTerminal() {
        return this == PREPARE_FAILED
                || this == DEPLOY_FAILED
                || this == INIT_FAILED
                || this == DESTROY_FAILED
                || this == DESTROYED;
    }
}
