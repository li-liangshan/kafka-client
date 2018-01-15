export declare type MessageHandler = (message: any) => void;
export declare type ErrorHandler = (error: any) => void;
export declare type OffsetOutOfRangeHandler = (err: string) => void;
/****************************************************************
 *** HighLevelConsumer has been deprecated in the latest version
 *** of Kafka (0.10.1) and is likely to be removed in the future.
 *** Please use the ConsumerGroup instead
 ****************************************************************/
export declare class ConsumerClient {
    closing: boolean;
    connecting: boolean;
    connected: boolean;
    consumerInstance: any;
    options: any;
    autoReconnect: boolean;
    kafkaClient: any;
    private isHighLevel;
    constructor(options: any, isHighLevel?: boolean, autoReconnect?: boolean);
    connect(): Promise<void>;
    onConnected(): Promise<any>;
    onReconnecting(): Promise<void>;
    isConnected(): boolean;
    close(force?: boolean): Promise<{}>;
    onClosed(force?: boolean): Promise<{}>;
    pause(): Promise<any>;
    resume(): Promise<any>;
    commit(force?: boolean): Promise<{}>;
    addTopics(topics: string | string[], fromOffset?: boolean): Promise<{}>;
    removeTopics(topics: string[]): Promise<{}>;
    setOffset(topic: string, partition?: number, offset?: number): Promise<any>;
    pauseTopics(topics: string[]): Promise<any>;
    resumeTopics(topics: string[]): Promise<any>;
    getTopicPayloads(): Promise<any>;
    consumeMessage(handler?: MessageHandler): Promise<any>;
    handleError(errorHandler: ErrorHandler): Promise<any>;
    consumeOffsetOutOfRange(offsetHandler: OffsetOutOfRangeHandler): Promise<any>;
    getConsumer(): Promise<any>;
    private createMessageHandler(handler);
    private createErrorHandler(errorHandler);
    private createOffsetOutOfRange(offsetHandler);
    private onMessage(message, handler);
    private onError(error, handler);
    private onOffsetOutOfRange(error, offsetHandler);
}
