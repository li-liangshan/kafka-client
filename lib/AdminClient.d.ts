export declare class AdminClient {
    closing: boolean;
    connecting: boolean;
    connected: boolean;
    private adminInstance;
    private autoReconnectCount;
    private kafkaClient;
    private options;
    private topics;
    constructor(client: any);
    connect(): Promise<any>;
    onConnected(): Promise<any>;
    onReconnecting(): Promise<any>;
    close(force?: boolean): Promise<{}>;
    onClosed(force?: boolean): Promise<{}>;
    listGroups(): Promise<{}>;
    describeGroups(consumerGroups: any): Promise<{}>;
}
