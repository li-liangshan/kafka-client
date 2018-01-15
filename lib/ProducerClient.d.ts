export declare class ProducerClient {
    private closing;
    private connecting;
    private connected;
    private isHighLevel;
    private autoReconnect;
    private producerInstance;
    private options;
    private kafkaClient;
    constructor(options: any, isHighLevel?: boolean, autoReconnect?: boolean);
    connect(): Promise<void>;
    onReconnecting(): Promise<void>;
    onConnected(): Promise<any>;
    close(): Promise<void>;
    onClosed(): Promise<{}>;
    createTopics(topics: string[], async?: boolean): Promise<{}>;
    send(payloads: any): Promise<{}>;
    isConnected(): boolean;
    private init(options, isHighLevel, autoReconnect);
}
