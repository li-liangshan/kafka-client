export declare class ConsumerStreamClient {
    closing: boolean;
    connecting: boolean;
    connected: boolean;
    private streamInstance;
    private autoReconnectCount;
    private kafkaClient;
    private options;
    private topics;
    constructor(client: any, topics: string | string[], options: any);
    connect(): Promise<any>;
    onConnected(): Promise<any>;
    onReconnecting(): Promise<any>;
    close(force?: boolean): Promise<{}>;
    onClosed(force?: boolean): Promise<{}>;
    createCommitStream(options: any): Promise<any>;
    consumeStreamMessage(handler: any): Promise<any>;
    isConnected(): boolean;
    private createMessageHandler(handler);
    private onMessage(message, handler);
    private init(client, topics, options);
}
