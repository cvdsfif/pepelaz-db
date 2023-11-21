import { ApiDefinition, ApiFunctionArgument, ApiFunctionReturnType, VoidField, unmarshal } from "pepelaz";
import { ITypedFacade } from "./typed-facade";

export const setConnectionTimeouts = (requestTimeout = 30000, connectTimeout = 10000, maxRetries = 3) => {
    require('aws-sdk/lib/maintenance_mode_message').suppress = true;
    const AWS = require("aws-sdk");

    AWS.config.update({
        maxRetries: maxRetries,
        httpOptions: {
            timeout: requestTimeout,
            connectTimeout: connectTimeout
        }
    });
}

export type ReportedEvent = {
    body: string
}

export type HandlerProps = {
    db?: () => ITypedFacade
}

export const interfaceHandler = async <T extends ApiDefinition, K extends keyof T>
    (
        template: T,
        implemented: K,
        handleFunction: (arg: ApiFunctionArgument<T, K>, props: HandlerProps) => Promise<ApiFunctionReturnType<T, K>>,
        event: ReportedEvent,
        props: HandlerProps
    ): Promise<ApiFunctionReturnType<T, K>> => {
    setConnectionTimeouts();
    const argument =
        template[implemented].arg instanceof VoidField ?
            void {} :
            unmarshal(template[implemented].arg, JSON.parse(event.body));
    return await handleFunction(argument, props);
}