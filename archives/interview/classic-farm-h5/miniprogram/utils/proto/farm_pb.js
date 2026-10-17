/*eslint-disable block-scoped-var, id-length, no-control-regex, no-magic-numbers, no-mixed-operators, no-prototype-builtins, no-redeclare, no-shadow, no-var, sort-vars, default-case, jsdoc/require-param*/
"use strict";

var $protobuf = require("./protobuf_min.js");

// Common aliases
var $Reader = $protobuf.Reader, $Writer = $protobuf.Writer, $util = $protobuf.util;
var $Object = $util.global.Object, $undefined = $util.global.undefined, $Error = $util.global.Error, $TypeError = $util.global.TypeError, $Number = $util.global.Number, $String = $util.global.String, $Array = $util.global.Array, $parseInt = $util.global.parseInt, $BigInt = $util.global.BigInt;

// Exported root namespace
var $root = $protobuf.roots["default"] || ($protobuf.roots["default"] = {});

$root.common = (function() {

    /**
     * Namespace common.
     * @exports common
     * @namespace
     */
    var common = {};

    /**
     * CommandStatus enum.
     * @name common.CommandStatus
     * @enum {number}
     * @property {number} COMMAND_STATUS_UNSPECIFIED=0 COMMAND_STATUS_UNSPECIFIED value
     * @property {number} SUCCEEDED=1 SUCCEEDED value
     * @property {number} PROCESSING=2 PROCESSING value
     * @property {number} NO_CHANGE=3 NO_CHANGE value
     * @property {number} REJECTED=4 REJECTED value
     * @property {number} UNAVAILABLE=5 UNAVAILABLE value
     * @property {number} ACCEPTED=6 ACCEPTED value
     */
    common.CommandStatus = (function() {
        var valuesById = $Object.create(null), values = $Object.create(valuesById);
        values[valuesById[0] = "COMMAND_STATUS_UNSPECIFIED"] = 0;
        values[valuesById[1] = "SUCCEEDED"] = 1;
        values[valuesById[2] = "PROCESSING"] = 2;
        values[valuesById[3] = "NO_CHANGE"] = 3;
        values[valuesById[4] = "REJECTED"] = 4;
        values[valuesById[5] = "UNAVAILABLE"] = 5;
        values[valuesById[6] = "ACCEPTED"] = 6;
        return values;
    })();

    /**
     * ErrorCode enum.
     * @name common.ErrorCode
     * @enum {number}
     * @property {number} ERROR_CODE_UNSPECIFIED=0 ERROR_CODE_UNSPECIFIED value
     * @property {number} OK=1 OK value
     * @property {number} ERR_BAD_FRAME=1001 ERR_BAD_FRAME value
     * @property {number} ERR_DECODE=1002 ERR_DECODE value
     * @property {number} ERR_UNSUPPORTED_VERSION=1003 ERR_UNSUPPORTED_VERSION value
     * @property {number} ERR_MESSAGE_TOO_LARGE=1004 ERR_MESSAGE_TOO_LARGE value
     * @property {number} ERR_UNKNOWN_COMMAND=1005 ERR_UNKNOWN_COMMAND value
     * @property {number} ERR_RATE_LIMITED=1006 ERR_RATE_LIMITED value
     * @property {number} ERR_UNAUTHENTICATED=2001 ERR_UNAUTHENTICATED value
     * @property {number} ERR_SESSION_EXPIRED=2002 ERR_SESSION_EXPIRED value
     * @property {number} ERR_SESSION_PERMIT_INVALID=2003 ERR_SESSION_PERMIT_INVALID value
     * @property {number} ERR_LOGIN_ELSEWHERE=2004 ERR_LOGIN_ELSEWHERE value
     * @property {number} ERR_CONNECTION_TICKET_INVALID=2005 ERR_CONNECTION_TICKET_INVALID value
     * @property {number} ERR_NOT_FRIEND=3001 ERR_NOT_FRIEND value
     * @property {number} ERR_FRIEND_PERMIT_INVALID=3002 ERR_FRIEND_PERMIT_INVALID value
     * @property {number} ERR_FRIEND_LIMIT=3003 ERR_FRIEND_LIMIT value
     * @property {number} ERR_SHARE_LINK_INVALID=3004 ERR_SHARE_LINK_INVALID value
     * @property {number} ERR_PERMISSION_DENIED=3005 ERR_PERMISSION_DENIED value
     * @property {number} ERR_INSUFFICIENT_COIN=4001 ERR_INSUFFICIENT_COIN value
     * @property {number} ERR_INSUFFICIENT_ITEM=4002 ERR_INSUFFICIENT_ITEM value
     * @property {number} ERR_OFFER_NOT_FOUND=4003 ERR_OFFER_NOT_FOUND value
     * @property {number} ERR_RULE_VERSION_MISMATCH=4004 ERR_RULE_VERSION_MISMATCH value
     * @property {number} ERR_PLOT_NOT_EMPTY=5001 ERR_PLOT_NOT_EMPTY value
     * @property {number} ERR_PLOT_EMPTY=5002 ERR_PLOT_EMPTY value
     * @property {number} ERR_CROP_NOT_MATURE=5003 ERR_CROP_NOT_MATURE value
     * @property {number} ERR_VERSION_CONFLICT=5004 ERR_VERSION_CONFLICT value
     * @property {number} ERR_PLOT_NOT_FOUND=5005 ERR_PLOT_NOT_FOUND value
     * @property {number} ERR_FARM_FULL=6001 ERR_FARM_FULL value
     * @property {number} ERR_NOT_IN_FARM=6002 ERR_NOT_IN_FARM value
     * @property {number} ERR_ACTOR_REDIRECT=6003 ERR_ACTOR_REDIRECT value
     * @property {number} ERR_DOWNLINK_OVERFLOW=6004 ERR_DOWNLINK_OVERFLOW value
     * @property {number} ERR_STEAL_ALREADY_DONE=7001 ERR_STEAL_ALREADY_DONE value
     * @property {number} ERR_STEAL_QUOTA_EXHAUSTED=7002 ERR_STEAL_QUOTA_EXHAUSTED value
     * @property {number} ERR_STEAL_BLOCKED_BY_PET=7003 ERR_STEAL_BLOCKED_BY_PET value
     * @property {number} ERR_SETTLEMENT_NOT_FOUND=7004 ERR_SETTLEMENT_NOT_FOUND value
     * @property {number} ERR_INTERNAL=9001 ERR_INTERNAL value
     * @property {number} ERR_STORE_CAS_CONFLICT=9002 ERR_STORE_CAS_CONFLICT value
     * @property {number} ERR_STORE_UNAVAILABLE=9003 ERR_STORE_UNAVAILABLE value
     * @property {number} ERR_TIMEOUT=9004 ERR_TIMEOUT value
     */
    common.ErrorCode = (function() {
        var valuesById = $Object.create(null), values = $Object.create(valuesById);
        values[valuesById[0] = "ERROR_CODE_UNSPECIFIED"] = 0;
        values[valuesById[1] = "OK"] = 1;
        values[valuesById[1001] = "ERR_BAD_FRAME"] = 1001;
        values[valuesById[1002] = "ERR_DECODE"] = 1002;
        values[valuesById[1003] = "ERR_UNSUPPORTED_VERSION"] = 1003;
        values[valuesById[1004] = "ERR_MESSAGE_TOO_LARGE"] = 1004;
        values[valuesById[1005] = "ERR_UNKNOWN_COMMAND"] = 1005;
        values[valuesById[1006] = "ERR_RATE_LIMITED"] = 1006;
        values[valuesById[2001] = "ERR_UNAUTHENTICATED"] = 2001;
        values[valuesById[2002] = "ERR_SESSION_EXPIRED"] = 2002;
        values[valuesById[2003] = "ERR_SESSION_PERMIT_INVALID"] = 2003;
        values[valuesById[2004] = "ERR_LOGIN_ELSEWHERE"] = 2004;
        values[valuesById[2005] = "ERR_CONNECTION_TICKET_INVALID"] = 2005;
        values[valuesById[3001] = "ERR_NOT_FRIEND"] = 3001;
        values[valuesById[3002] = "ERR_FRIEND_PERMIT_INVALID"] = 3002;
        values[valuesById[3003] = "ERR_FRIEND_LIMIT"] = 3003;
        values[valuesById[3004] = "ERR_SHARE_LINK_INVALID"] = 3004;
        values[valuesById[3005] = "ERR_PERMISSION_DENIED"] = 3005;
        values[valuesById[4001] = "ERR_INSUFFICIENT_COIN"] = 4001;
        values[valuesById[4002] = "ERR_INSUFFICIENT_ITEM"] = 4002;
        values[valuesById[4003] = "ERR_OFFER_NOT_FOUND"] = 4003;
        values[valuesById[4004] = "ERR_RULE_VERSION_MISMATCH"] = 4004;
        values[valuesById[5001] = "ERR_PLOT_NOT_EMPTY"] = 5001;
        values[valuesById[5002] = "ERR_PLOT_EMPTY"] = 5002;
        values[valuesById[5003] = "ERR_CROP_NOT_MATURE"] = 5003;
        values[valuesById[5004] = "ERR_VERSION_CONFLICT"] = 5004;
        values[valuesById[5005] = "ERR_PLOT_NOT_FOUND"] = 5005;
        values[valuesById[6001] = "ERR_FARM_FULL"] = 6001;
        values[valuesById[6002] = "ERR_NOT_IN_FARM"] = 6002;
        values[valuesById[6003] = "ERR_ACTOR_REDIRECT"] = 6003;
        values[valuesById[6004] = "ERR_DOWNLINK_OVERFLOW"] = 6004;
        values[valuesById[7001] = "ERR_STEAL_ALREADY_DONE"] = 7001;
        values[valuesById[7002] = "ERR_STEAL_QUOTA_EXHAUSTED"] = 7002;
        values[valuesById[7003] = "ERR_STEAL_BLOCKED_BY_PET"] = 7003;
        values[valuesById[7004] = "ERR_SETTLEMENT_NOT_FOUND"] = 7004;
        values[valuesById[9001] = "ERR_INTERNAL"] = 9001;
        values[valuesById[9002] = "ERR_STORE_CAS_CONFLICT"] = 9002;
        values[valuesById[9003] = "ERR_STORE_UNAVAILABLE"] = 9003;
        values[valuesById[9004] = "ERR_TIMEOUT"] = 9004;
        return values;
    })();

    /**
     * MessageType enum.
     * @name common.MessageType
     * @enum {number}
     * @property {number} MESSAGE_TYPE_UNSPECIFIED=0 MESSAGE_TYPE_UNSPECIFIED value
     * @property {number} REQUEST=1 REQUEST value
     * @property {number} RESPONSE=2 RESPONSE value
     * @property {number} NOTIFY=3 NOTIFY value
     */
    common.MessageType = (function() {
        var valuesById = $Object.create(null), values = $Object.create(valuesById);
        values[valuesById[0] = "MESSAGE_TYPE_UNSPECIFIED"] = 0;
        values[valuesById[1] = "REQUEST"] = 1;
        values[valuesById[2] = "RESPONSE"] = 2;
        values[valuesById[3] = "NOTIFY"] = 3;
        return values;
    })();

    /**
     * FrameKind enum.
     * @name common.FrameKind
     * @enum {number}
     * @property {number} FRAME_KIND_UNSPECIFIED=0 FRAME_KIND_UNSPECIFIED value
     * @property {number} FRAME_KIND_CLIENT_HELLO=1 FRAME_KIND_CLIENT_HELLO value
     * @property {number} FRAME_KIND_SERVER_HELLO=2 FRAME_KIND_SERVER_HELLO value
     * @property {number} FRAME_KIND_CLIENT_COMMAND=3 FRAME_KIND_CLIENT_COMMAND value
     * @property {number} FRAME_KIND_COMMAND_RESULT=4 FRAME_KIND_COMMAND_RESULT value
     * @property {number} FRAME_KIND_SERVER_PUSH=5 FRAME_KIND_SERVER_PUSH value
     * @property {number} FRAME_KIND_CONNECTION_ERROR=6 FRAME_KIND_CONNECTION_ERROR value
     */
    common.FrameKind = (function() {
        var valuesById = $Object.create(null), values = $Object.create(valuesById);
        values[valuesById[0] = "FRAME_KIND_UNSPECIFIED"] = 0;
        values[valuesById[1] = "FRAME_KIND_CLIENT_HELLO"] = 1;
        values[valuesById[2] = "FRAME_KIND_SERVER_HELLO"] = 2;
        values[valuesById[3] = "FRAME_KIND_CLIENT_COMMAND"] = 3;
        values[valuesById[4] = "FRAME_KIND_COMMAND_RESULT"] = 4;
        values[valuesById[5] = "FRAME_KIND_SERVER_PUSH"] = 5;
        values[valuesById[6] = "FRAME_KIND_CONNECTION_ERROR"] = 6;
        return values;
    })();

    common.FarmFrame = (function() {

        /**
         * Properties of a FarmFrame.
         * @typedef {Object} common.FarmFrame.$Properties
         * @property {number|null} [protocolVersion] FarmFrame protocolVersion
         * @property {common.FrameKind|null} [kind] FarmFrame kind
         * @property {number|null} [connectionSeq] FarmFrame connectionSeq
         * @property {Uint8Array|null} [body] FarmFrame body
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */

        /**
         * Properties of a FarmFrame.
         * @memberof common
         * @interface IFarmFrame
         * @augments common.FarmFrame.$Properties
         * @deprecated Use common.FarmFrame.$Properties instead.
         */

        /**
         * Shape of a FarmFrame.
         * @typedef {common.FarmFrame.$Properties} common.FarmFrame.$Shape
         */

        /**
         * Constructs a new FarmFrame.
         * @memberof common
         * @classdesc Represents a FarmFrame.
         * @constructor
         * @param {common.FarmFrame.$Properties=} [properties] Properties to set
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */
        var FarmFrame = function (properties) {
            if (properties)
                for (var keys = $Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null && keys[i] !== "__proto__")
                        this[keys[i]] = properties[keys[i]];
        };

        /**
         * FarmFrame protocolVersion.
         * @member {number} protocolVersion
         * @memberof common.FarmFrame
         * @instance
         */
        FarmFrame.prototype.protocolVersion = 0;

        /**
         * FarmFrame kind.
         * @member {common.FrameKind} kind
         * @memberof common.FarmFrame
         * @instance
         */
        FarmFrame.prototype.kind = 0;

        /**
         * FarmFrame connectionSeq.
         * @member {number} connectionSeq
         * @memberof common.FarmFrame
         * @instance
         */
        FarmFrame.prototype.connectionSeq = 0;

        /**
         * FarmFrame body.
         * @member {Uint8Array} body
         * @memberof common.FarmFrame
         * @instance
         */
        FarmFrame.prototype.body = $util.newBuffer([]);

        /**
         * Creates a new FarmFrame instance using the specified properties.
         * @function create
         * @memberof common.FarmFrame
         * @static
         * @param {common.FarmFrame.$Properties=} [properties] Properties to set
         * @returns {common.FarmFrame} FarmFrame instance
         * @type {{
         *   (properties: common.FarmFrame.$Shape): common.FarmFrame & common.FarmFrame.$Shape;
         *   (properties?: common.FarmFrame.$Properties): common.FarmFrame;
         * }}
         */
        FarmFrame.create = function(properties) {
            return new FarmFrame(properties);
        };

        /**
         * Encodes the specified FarmFrame message. Does not implicitly {@link common.FarmFrame.verify|verify} messages.
         * @function encode
         * @memberof common.FarmFrame
         * @static
         * @param {common.FarmFrame.$Properties} message FarmFrame message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        FarmFrame.encode = function (message, writer, _depth) {
            if (!writer)
                writer = $Writer.create();
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            if (message.protocolVersion != null && $Object.hasOwnProperty.call(message, "protocolVersion") && message.protocolVersion !== 0)
                writer.uint32(/* id 1, wireType 0 =*/8).uint32(message.protocolVersion);
            if (message.kind != null && $Object.hasOwnProperty.call(message, "kind") && message.kind !== 0)
                writer.uint32(/* id 2, wireType 0 =*/16).int32(message.kind);
            if (message.connectionSeq != null && $Object.hasOwnProperty.call(message, "connectionSeq") && message.connectionSeq !== 0)
                writer.uint32(/* id 3, wireType 0 =*/24).uint32(message.connectionSeq);
            if (message.body != null && $Object.hasOwnProperty.call(message, "body") && message.body.length)
                writer.uint32(/* id 15, wireType 2 =*/122).bytes(message.body);
            if (message.$unknowns != null && $Object.hasOwnProperty.call(message, "$unknowns"))
                for (var i = 0; i < message.$unknowns.length; ++i)
                    writer.raw(message.$unknowns[i]);
            return writer;
        };

        /**
         * Encodes the specified FarmFrame message, length delimited. Does not implicitly {@link common.FarmFrame.verify|verify} messages.
         * @function encodeDelimited
         * @memberof common.FarmFrame
         * @static
         * @param {common.FarmFrame.$Properties} message FarmFrame message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        FarmFrame.encodeDelimited = function(message, writer) {
            return this.encode(message, (writer || $Writer.create()).fork()).ldelim();
        };

        /**
         * Decodes a FarmFrame message from the specified reader or buffer.
         * @function decode
         * @memberof common.FarmFrame
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {common.FarmFrame & common.FarmFrame.$Shape} FarmFrame
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        FarmFrame.decode = function (reader, length, _end, _depth, _target) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $Reader.recursionLimit)
                throw $Error("max depth exceeded");
            var end = length === $undefined ? reader.len : reader.pos + length, message = _target || new $root.common.FarmFrame(), value;
            while (reader.pos < end) {
                var start = reader.pos;
                var tag = reader.tag();
                if (tag === _end) {
                    _end = $undefined;
                    break;
                }
                var wireType = tag & 7;
                switch (tag >>>= 3) {
                case 1: {
                        if (wireType !== 0)
                            break;
                        if (value = reader.uint32())
                            message.protocolVersion = value;
                        else
                            delete message.protocolVersion;
                        continue;
                    }
                case 2: {
                        if (wireType !== 0)
                            break;
                        if (value = reader.int32())
                            message.kind = value;
                        else
                            delete message.kind;
                        continue;
                    }
                case 3: {
                        if (wireType !== 0)
                            break;
                        if (value = reader.uint32())
                            message.connectionSeq = value;
                        else
                            delete message.connectionSeq;
                        continue;
                    }
                case 15: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.bytes()).length)
                            message.body = value;
                        else
                            delete message.body;
                        continue;
                    }
                }
                reader.skipType(wireType, _depth, tag);
                if (!reader.discardUnknown) {
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
            }
            if (_end !== $undefined)
                throw $Error("missing end group");
            return message;
        };

        /**
         * Decodes a FarmFrame message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof common.FarmFrame
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {common.FarmFrame & common.FarmFrame.$Shape} FarmFrame
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        FarmFrame.decodeDelimited = function(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a FarmFrame message.
         * @function verify
         * @memberof common.FarmFrame
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        FarmFrame.verify = function (message, _depth) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                return "max depth exceeded";
            if (message.protocolVersion != null && $Object.hasOwnProperty.call(message, "protocolVersion"))
                if (!$util.isInteger(message.protocolVersion))
                    return "protocolVersion: integer expected";
            if (message.kind != null && $Object.hasOwnProperty.call(message, "kind"))
                if (typeof message.kind !== "number" || (message.kind | 0) !== message.kind)
                    return "kind: enum value expected";
            if (message.connectionSeq != null && $Object.hasOwnProperty.call(message, "connectionSeq"))
                if (!$util.isInteger(message.connectionSeq))
                    return "connectionSeq: integer expected";
            if (message.body != null && $Object.hasOwnProperty.call(message, "body"))
                if (!(message.body && typeof message.body.length === "number" || $util.isString(message.body)))
                    return "body: buffer expected";
            return null;
        };

        /**
         * Creates a FarmFrame message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof common.FarmFrame
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {common.FarmFrame} FarmFrame
         */
        FarmFrame.fromObject = function (object, _depth) {
            if (object instanceof $root.common.FarmFrame)
                return object;
            if (!$util.isObject(object))
                throw $TypeError(".common.FarmFrame: object expected");
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var message = new $root.common.FarmFrame();
            if (object.protocolVersion != null)
                if ($Number(object.protocolVersion) !== 0)
                    message.protocolVersion = object.protocolVersion >>> 0;
            if (object.kind !== 0 && (typeof object.kind !== "string" || $root.common.FrameKind[object.kind] !== 0))
                switch (object.kind) {
                case "FRAME_KIND_UNSPECIFIED":
                case 0:
                    message.kind = 0;
                    break;
                case "FRAME_KIND_CLIENT_HELLO":
                case 1:
                    message.kind = 1;
                    break;
                case "FRAME_KIND_SERVER_HELLO":
                case 2:
                    message.kind = 2;
                    break;
                case "FRAME_KIND_CLIENT_COMMAND":
                case 3:
                    message.kind = 3;
                    break;
                case "FRAME_KIND_COMMAND_RESULT":
                case 4:
                    message.kind = 4;
                    break;
                case "FRAME_KIND_SERVER_PUSH":
                case 5:
                    message.kind = 5;
                    break;
                case "FRAME_KIND_CONNECTION_ERROR":
                case 6:
                    message.kind = 6;
                    break;
                default:
                    if (typeof object.kind === "number" && (object.kind | 0) === object.kind)
                        message.kind = object.kind;
                }
            if (object.connectionSeq != null)
                if ($Number(object.connectionSeq) !== 0)
                    message.connectionSeq = object.connectionSeq >>> 0;
            if (object.body != null)
                if (object.body.length)
                    if (typeof object.body === "string")
                        $util.base64.decode(object.body, message.body = $util.newBuffer($util.base64.length(object.body)), 0);
                    else if (object.body.length >= 0)
                        message.body = object.body;
            return message;
        };

        /**
         * Creates a plain object from a FarmFrame message. Also converts values to other types if specified.
         * @function toObject
         * @memberof common.FarmFrame
         * @static
         * @param {common.FarmFrame} message FarmFrame
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        FarmFrame.toObject = function (message, options, _depth) {
            if (!options)
                options = {};
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var object = {};
            if (options.defaults) {
                object.protocolVersion = 0;
                object.kind = options.enums === $String ? "FRAME_KIND_UNSPECIFIED" : 0;
                object.connectionSeq = 0;
                if (options.bytes === $String)
                    object.body = "";
                else {
                    object.body = [];
                    if (options.bytes !== $Array)
                        object.body = $util.newBuffer(object.body);
                }
            }
            if (message.protocolVersion != null && $Object.hasOwnProperty.call(message, "protocolVersion"))
                object.protocolVersion = message.protocolVersion;
            if (message.kind != null && $Object.hasOwnProperty.call(message, "kind"))
                object.kind = options.enums === $String ? $root.common.FrameKind[message.kind] === $undefined ? message.kind : $root.common.FrameKind[message.kind] : message.kind;
            if (message.connectionSeq != null && $Object.hasOwnProperty.call(message, "connectionSeq"))
                object.connectionSeq = message.connectionSeq;
            if (message.body != null && $Object.hasOwnProperty.call(message, "body"))
                object.body = options.bytes === $String ? $util.base64.encode(message.body, 0, message.body.length) : options.bytes === $Array ? $Array.prototype.slice.call(message.body) : message.body;
            return object;
        };

        /**
         * Converts this FarmFrame to JSON.
         * @function toJSON
         * @memberof common.FarmFrame
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        FarmFrame.prototype.toJSON = function() {
            return FarmFrame.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the type url for FarmFrame
         * @function getTypeUrl
         * @memberof common.FarmFrame
         * @static
         * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
         * @returns {string} The type url
         */
        FarmFrame.getTypeUrl = function(prefix) {
            if (prefix === $undefined)
                prefix = "type.googleapis.com";
            return prefix + "/common.FarmFrame";
        };

        return FarmFrame;
    })();

    common.ClientHello = (function() {

        /**
         * Properties of a ClientHello.
         * @typedef {Object} common.ClientHello.$Properties
         * @property {string|null} [connectionTicket] ClientHello connectionTicket
         * @property {string|null} [clientBuild] ClientHello clientBuild
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */

        /**
         * Properties of a ClientHello.
         * @memberof common
         * @interface IClientHello
         * @augments common.ClientHello.$Properties
         * @deprecated Use common.ClientHello.$Properties instead.
         */

        /**
         * Shape of a ClientHello.
         * @typedef {common.ClientHello.$Properties} common.ClientHello.$Shape
         */

        /**
         * Constructs a new ClientHello.
         * @memberof common
         * @classdesc Represents a ClientHello.
         * @constructor
         * @param {common.ClientHello.$Properties=} [properties] Properties to set
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */
        var ClientHello = function (properties) {
            if (properties)
                for (var keys = $Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null && keys[i] !== "__proto__")
                        this[keys[i]] = properties[keys[i]];
        };

        /**
         * ClientHello connectionTicket.
         * @member {string} connectionTicket
         * @memberof common.ClientHello
         * @instance
         */
        ClientHello.prototype.connectionTicket = "";

        /**
         * ClientHello clientBuild.
         * @member {string} clientBuild
         * @memberof common.ClientHello
         * @instance
         */
        ClientHello.prototype.clientBuild = "";

        /**
         * Creates a new ClientHello instance using the specified properties.
         * @function create
         * @memberof common.ClientHello
         * @static
         * @param {common.ClientHello.$Properties=} [properties] Properties to set
         * @returns {common.ClientHello} ClientHello instance
         * @type {{
         *   (properties: common.ClientHello.$Shape): common.ClientHello & common.ClientHello.$Shape;
         *   (properties?: common.ClientHello.$Properties): common.ClientHello;
         * }}
         */
        ClientHello.create = function(properties) {
            return new ClientHello(properties);
        };

        /**
         * Encodes the specified ClientHello message. Does not implicitly {@link common.ClientHello.verify|verify} messages.
         * @function encode
         * @memberof common.ClientHello
         * @static
         * @param {common.ClientHello.$Properties} message ClientHello message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ClientHello.encode = function (message, writer, _depth) {
            if (!writer)
                writer = $Writer.create();
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            if (message.connectionTicket != null && $Object.hasOwnProperty.call(message, "connectionTicket") && message.connectionTicket !== "")
                writer.uint32(/* id 1, wireType 2 =*/10).string(message.connectionTicket);
            if (message.clientBuild != null && $Object.hasOwnProperty.call(message, "clientBuild") && message.clientBuild !== "")
                writer.uint32(/* id 3, wireType 2 =*/26).string(message.clientBuild);
            if (message.$unknowns != null && $Object.hasOwnProperty.call(message, "$unknowns"))
                for (var i = 0; i < message.$unknowns.length; ++i)
                    writer.raw(message.$unknowns[i]);
            return writer;
        };

        /**
         * Encodes the specified ClientHello message, length delimited. Does not implicitly {@link common.ClientHello.verify|verify} messages.
         * @function encodeDelimited
         * @memberof common.ClientHello
         * @static
         * @param {common.ClientHello.$Properties} message ClientHello message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ClientHello.encodeDelimited = function(message, writer) {
            return this.encode(message, (writer || $Writer.create()).fork()).ldelim();
        };

        /**
         * Decodes a ClientHello message from the specified reader or buffer.
         * @function decode
         * @memberof common.ClientHello
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {common.ClientHello & common.ClientHello.$Shape} ClientHello
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ClientHello.decode = function (reader, length, _end, _depth, _target) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $Reader.recursionLimit)
                throw $Error("max depth exceeded");
            var end = length === $undefined ? reader.len : reader.pos + length, message = _target || new $root.common.ClientHello(), value;
            while (reader.pos < end) {
                var start = reader.pos;
                var tag = reader.tag();
                if (tag === _end) {
                    _end = $undefined;
                    break;
                }
                var wireType = tag & 7;
                switch (tag >>>= 3) {
                case 1: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.connectionTicket = value;
                        else
                            delete message.connectionTicket;
                        continue;
                    }
                case 3: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.clientBuild = value;
                        else
                            delete message.clientBuild;
                        continue;
                    }
                }
                reader.skipType(wireType, _depth, tag);
                if (!reader.discardUnknown) {
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
            }
            if (_end !== $undefined)
                throw $Error("missing end group");
            return message;
        };

        /**
         * Decodes a ClientHello message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof common.ClientHello
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {common.ClientHello & common.ClientHello.$Shape} ClientHello
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ClientHello.decodeDelimited = function(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a ClientHello message.
         * @function verify
         * @memberof common.ClientHello
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        ClientHello.verify = function (message, _depth) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                return "max depth exceeded";
            if (message.connectionTicket != null && $Object.hasOwnProperty.call(message, "connectionTicket"))
                if (!$util.isString(message.connectionTicket))
                    return "connectionTicket: string expected";
            if (message.clientBuild != null && $Object.hasOwnProperty.call(message, "clientBuild"))
                if (!$util.isString(message.clientBuild))
                    return "clientBuild: string expected";
            return null;
        };

        /**
         * Creates a ClientHello message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof common.ClientHello
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {common.ClientHello} ClientHello
         */
        ClientHello.fromObject = function (object, _depth) {
            if (object instanceof $root.common.ClientHello)
                return object;
            if (!$util.isObject(object))
                throw $TypeError(".common.ClientHello: object expected");
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var message = new $root.common.ClientHello();
            if (object.connectionTicket != null)
                if (typeof object.connectionTicket !== "string" || object.connectionTicket.length)
                    message.connectionTicket = $String(object.connectionTicket);
            if (object.clientBuild != null)
                if (typeof object.clientBuild !== "string" || object.clientBuild.length)
                    message.clientBuild = $String(object.clientBuild);
            return message;
        };

        /**
         * Creates a plain object from a ClientHello message. Also converts values to other types if specified.
         * @function toObject
         * @memberof common.ClientHello
         * @static
         * @param {common.ClientHello} message ClientHello
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        ClientHello.toObject = function (message, options, _depth) {
            if (!options)
                options = {};
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var object = {};
            if (options.defaults) {
                object.connectionTicket = "";
                object.clientBuild = "";
            }
            if (message.connectionTicket != null && $Object.hasOwnProperty.call(message, "connectionTicket"))
                object.connectionTicket = message.connectionTicket;
            if (message.clientBuild != null && $Object.hasOwnProperty.call(message, "clientBuild"))
                object.clientBuild = message.clientBuild;
            return object;
        };

        /**
         * Converts this ClientHello to JSON.
         * @function toJSON
         * @memberof common.ClientHello
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        ClientHello.prototype.toJSON = function() {
            return ClientHello.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the type url for ClientHello
         * @function getTypeUrl
         * @memberof common.ClientHello
         * @static
         * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
         * @returns {string} The type url
         */
        ClientHello.getTypeUrl = function(prefix) {
            if (prefix === $undefined)
                prefix = "type.googleapis.com";
            return prefix + "/common.ClientHello";
        };

        return ClientHello;
    })();

    common.ServerHello = (function() {

        /**
         * Properties of a ServerHello.
         * @typedef {Object} common.ServerHello.$Properties
         * @property {string|null} [sessionId] ServerHello sessionId
         * @property {number|Long|null} [activeSessionEpoch] ServerHello activeSessionEpoch
         * @property {number|Long|null} [sessionAuthorityEpoch] ServerHello sessionAuthorityEpoch
         * @property {number|Long|null} [serverTimeMs] ServerHello serverTimeMs
         * @property {number|null} [maxFrameBytes] ServerHello maxFrameBytes
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */

        /**
         * Properties of a ServerHello.
         * @memberof common
         * @interface IServerHello
         * @augments common.ServerHello.$Properties
         * @deprecated Use common.ServerHello.$Properties instead.
         */

        /**
         * Shape of a ServerHello.
         * @typedef {common.ServerHello.$Properties} common.ServerHello.$Shape
         */

        /**
         * Constructs a new ServerHello.
         * @memberof common
         * @classdesc Represents a ServerHello.
         * @constructor
         * @param {common.ServerHello.$Properties=} [properties] Properties to set
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */
        var ServerHello = function (properties) {
            if (properties)
                for (var keys = $Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null && keys[i] !== "__proto__")
                        this[keys[i]] = properties[keys[i]];
        };

        /**
         * ServerHello sessionId.
         * @member {string} sessionId
         * @memberof common.ServerHello
         * @instance
         */
        ServerHello.prototype.sessionId = "";

        /**
         * ServerHello activeSessionEpoch.
         * @member {number|Long} activeSessionEpoch
         * @memberof common.ServerHello
         * @instance
         */
        ServerHello.prototype.activeSessionEpoch = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * ServerHello sessionAuthorityEpoch.
         * @member {number|Long} sessionAuthorityEpoch
         * @memberof common.ServerHello
         * @instance
         */
        ServerHello.prototype.sessionAuthorityEpoch = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * ServerHello serverTimeMs.
         * @member {number|Long} serverTimeMs
         * @memberof common.ServerHello
         * @instance
         */
        ServerHello.prototype.serverTimeMs = $util.Long ? $util.Long.fromBits(0,0,false) : 0;

        /**
         * ServerHello maxFrameBytes.
         * @member {number} maxFrameBytes
         * @memberof common.ServerHello
         * @instance
         */
        ServerHello.prototype.maxFrameBytes = 0;

        /**
         * Creates a new ServerHello instance using the specified properties.
         * @function create
         * @memberof common.ServerHello
         * @static
         * @param {common.ServerHello.$Properties=} [properties] Properties to set
         * @returns {common.ServerHello} ServerHello instance
         * @type {{
         *   (properties: common.ServerHello.$Shape): common.ServerHello & common.ServerHello.$Shape;
         *   (properties?: common.ServerHello.$Properties): common.ServerHello;
         * }}
         */
        ServerHello.create = function(properties) {
            return new ServerHello(properties);
        };

        /**
         * Encodes the specified ServerHello message. Does not implicitly {@link common.ServerHello.verify|verify} messages.
         * @function encode
         * @memberof common.ServerHello
         * @static
         * @param {common.ServerHello.$Properties} message ServerHello message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ServerHello.encode = function (message, writer, _depth) {
            if (!writer)
                writer = $Writer.create();
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            if (message.sessionId != null && $Object.hasOwnProperty.call(message, "sessionId") && message.sessionId !== "")
                writer.uint32(/* id 1, wireType 2 =*/10).string(message.sessionId);
            if (message.activeSessionEpoch != null && $Object.hasOwnProperty.call(message, "activeSessionEpoch") && (typeof message.activeSessionEpoch === "object" ? message.activeSessionEpoch.low || message.activeSessionEpoch.high : message.activeSessionEpoch !== 0))
                writer.uint32(/* id 2, wireType 0 =*/16).uint64(message.activeSessionEpoch);
            if (message.sessionAuthorityEpoch != null && $Object.hasOwnProperty.call(message, "sessionAuthorityEpoch") && (typeof message.sessionAuthorityEpoch === "object" ? message.sessionAuthorityEpoch.low || message.sessionAuthorityEpoch.high : message.sessionAuthorityEpoch !== 0))
                writer.uint32(/* id 3, wireType 0 =*/24).uint64(message.sessionAuthorityEpoch);
            if (message.serverTimeMs != null && $Object.hasOwnProperty.call(message, "serverTimeMs") && (typeof message.serverTimeMs === "object" ? message.serverTimeMs.low || message.serverTimeMs.high : message.serverTimeMs !== 0))
                writer.uint32(/* id 4, wireType 0 =*/32).int64(message.serverTimeMs);
            if (message.maxFrameBytes != null && $Object.hasOwnProperty.call(message, "maxFrameBytes") && message.maxFrameBytes !== 0)
                writer.uint32(/* id 6, wireType 0 =*/48).uint32(message.maxFrameBytes);
            if (message.$unknowns != null && $Object.hasOwnProperty.call(message, "$unknowns"))
                for (var i = 0; i < message.$unknowns.length; ++i)
                    writer.raw(message.$unknowns[i]);
            return writer;
        };

        /**
         * Encodes the specified ServerHello message, length delimited. Does not implicitly {@link common.ServerHello.verify|verify} messages.
         * @function encodeDelimited
         * @memberof common.ServerHello
         * @static
         * @param {common.ServerHello.$Properties} message ServerHello message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ServerHello.encodeDelimited = function(message, writer) {
            return this.encode(message, (writer || $Writer.create()).fork()).ldelim();
        };

        /**
         * Decodes a ServerHello message from the specified reader or buffer.
         * @function decode
         * @memberof common.ServerHello
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {common.ServerHello & common.ServerHello.$Shape} ServerHello
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ServerHello.decode = function (reader, length, _end, _depth, _target) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $Reader.recursionLimit)
                throw $Error("max depth exceeded");
            var end = length === $undefined ? reader.len : reader.pos + length, message = _target || new $root.common.ServerHello(), value;
            while (reader.pos < end) {
                var start = reader.pos;
                var tag = reader.tag();
                if (tag === _end) {
                    _end = $undefined;
                    break;
                }
                var wireType = tag & 7;
                switch (tag >>>= 3) {
                case 1: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.sessionId = value;
                        else
                            delete message.sessionId;
                        continue;
                    }
                case 2: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.activeSessionEpoch = value;
                        else
                            delete message.activeSessionEpoch;
                        continue;
                    }
                case 3: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.sessionAuthorityEpoch = value;
                        else
                            delete message.sessionAuthorityEpoch;
                        continue;
                    }
                case 4: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.int64()) === "object" ? value.low || value.high : value !== 0)
                            message.serverTimeMs = value;
                        else
                            delete message.serverTimeMs;
                        continue;
                    }
                case 6: {
                        if (wireType !== 0)
                            break;
                        if (value = reader.uint32())
                            message.maxFrameBytes = value;
                        else
                            delete message.maxFrameBytes;
                        continue;
                    }
                }
                reader.skipType(wireType, _depth, tag);
                if (!reader.discardUnknown) {
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
            }
            if (_end !== $undefined)
                throw $Error("missing end group");
            return message;
        };

        /**
         * Decodes a ServerHello message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof common.ServerHello
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {common.ServerHello & common.ServerHello.$Shape} ServerHello
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ServerHello.decodeDelimited = function(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a ServerHello message.
         * @function verify
         * @memberof common.ServerHello
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        ServerHello.verify = function (message, _depth) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                return "max depth exceeded";
            if (message.sessionId != null && $Object.hasOwnProperty.call(message, "sessionId"))
                if (!$util.isString(message.sessionId))
                    return "sessionId: string expected";
            if (message.activeSessionEpoch != null && $Object.hasOwnProperty.call(message, "activeSessionEpoch"))
                if (!$util.isInteger(message.activeSessionEpoch) && !(message.activeSessionEpoch && $util.isInteger(message.activeSessionEpoch.low) && $util.isInteger(message.activeSessionEpoch.high)))
                    return "activeSessionEpoch: integer|Long expected";
            if (message.sessionAuthorityEpoch != null && $Object.hasOwnProperty.call(message, "sessionAuthorityEpoch"))
                if (!$util.isInteger(message.sessionAuthorityEpoch) && !(message.sessionAuthorityEpoch && $util.isInteger(message.sessionAuthorityEpoch.low) && $util.isInteger(message.sessionAuthorityEpoch.high)))
                    return "sessionAuthorityEpoch: integer|Long expected";
            if (message.serverTimeMs != null && $Object.hasOwnProperty.call(message, "serverTimeMs"))
                if (!$util.isInteger(message.serverTimeMs) && !(message.serverTimeMs && $util.isInteger(message.serverTimeMs.low) && $util.isInteger(message.serverTimeMs.high)))
                    return "serverTimeMs: integer|Long expected";
            if (message.maxFrameBytes != null && $Object.hasOwnProperty.call(message, "maxFrameBytes"))
                if (!$util.isInteger(message.maxFrameBytes))
                    return "maxFrameBytes: integer expected";
            return null;
        };

        /**
         * Creates a ServerHello message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof common.ServerHello
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {common.ServerHello} ServerHello
         */
        ServerHello.fromObject = function (object, _depth) {
            if (object instanceof $root.common.ServerHello)
                return object;
            if (!$util.isObject(object))
                throw $TypeError(".common.ServerHello: object expected");
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var message = new $root.common.ServerHello();
            if (object.sessionId != null)
                if (typeof object.sessionId !== "string" || object.sessionId.length)
                    message.sessionId = $String(object.sessionId);
            if (object.activeSessionEpoch != null)
                if (typeof object.activeSessionEpoch === "object" ? object.activeSessionEpoch.low || object.activeSessionEpoch.high : $Number(object.activeSessionEpoch) !== 0)
                    if ($util.Long)
                        message.activeSessionEpoch = $util.Long.fromValue(object.activeSessionEpoch, true);
                    else if (typeof object.activeSessionEpoch === "string")
                        message.activeSessionEpoch = $parseInt(object.activeSessionEpoch, 10);
                    else if (typeof object.activeSessionEpoch === "number")
                        message.activeSessionEpoch = object.activeSessionEpoch;
                    else if (typeof object.activeSessionEpoch === "object")
                        message.activeSessionEpoch = new $util.LongBits(object.activeSessionEpoch.low >>> 0, object.activeSessionEpoch.high >>> 0).toNumber(true);
            if (object.sessionAuthorityEpoch != null)
                if (typeof object.sessionAuthorityEpoch === "object" ? object.sessionAuthorityEpoch.low || object.sessionAuthorityEpoch.high : $Number(object.sessionAuthorityEpoch) !== 0)
                    if ($util.Long)
                        message.sessionAuthorityEpoch = $util.Long.fromValue(object.sessionAuthorityEpoch, true);
                    else if (typeof object.sessionAuthorityEpoch === "string")
                        message.sessionAuthorityEpoch = $parseInt(object.sessionAuthorityEpoch, 10);
                    else if (typeof object.sessionAuthorityEpoch === "number")
                        message.sessionAuthorityEpoch = object.sessionAuthorityEpoch;
                    else if (typeof object.sessionAuthorityEpoch === "object")
                        message.sessionAuthorityEpoch = new $util.LongBits(object.sessionAuthorityEpoch.low >>> 0, object.sessionAuthorityEpoch.high >>> 0).toNumber(true);
            if (object.serverTimeMs != null)
                if (typeof object.serverTimeMs === "object" ? object.serverTimeMs.low || object.serverTimeMs.high : $Number(object.serverTimeMs) !== 0)
                    if ($util.Long)
                        message.serverTimeMs = $util.Long.fromValue(object.serverTimeMs, false);
                    else if (typeof object.serverTimeMs === "string")
                        message.serverTimeMs = $parseInt(object.serverTimeMs, 10);
                    else if (typeof object.serverTimeMs === "number")
                        message.serverTimeMs = object.serverTimeMs;
                    else if (typeof object.serverTimeMs === "object")
                        message.serverTimeMs = new $util.LongBits(object.serverTimeMs.low >>> 0, object.serverTimeMs.high >>> 0).toNumber();
            if (object.maxFrameBytes != null)
                if ($Number(object.maxFrameBytes) !== 0)
                    message.maxFrameBytes = object.maxFrameBytes >>> 0;
            return message;
        };

        /**
         * Creates a plain object from a ServerHello message. Also converts values to other types if specified.
         * @function toObject
         * @memberof common.ServerHello
         * @static
         * @param {common.ServerHello} message ServerHello
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        ServerHello.toObject = function (message, options, _depth) {
            if (!options)
                options = {};
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var object = {};
            if (options.defaults) {
                object.sessionId = "";
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.activeSessionEpoch = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.activeSessionEpoch = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.sessionAuthorityEpoch = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.sessionAuthorityEpoch = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                if ($util.Long) {
                    var long = new $util.Long(0, 0, false);
                    object.serverTimeMs = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.serverTimeMs = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                object.maxFrameBytes = 0;
            }
            if (message.sessionId != null && $Object.hasOwnProperty.call(message, "sessionId"))
                object.sessionId = message.sessionId;
            if (message.activeSessionEpoch != null && $Object.hasOwnProperty.call(message, "activeSessionEpoch"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.activeSessionEpoch = typeof message.activeSessionEpoch === "number" ? $BigInt(message.activeSessionEpoch) : $util.Long.fromBits(message.activeSessionEpoch.low >>> 0, message.activeSessionEpoch.high >>> 0, true).toBigInt();
                else if (typeof message.activeSessionEpoch === "number")
                    object.activeSessionEpoch = options.longs === $String ? $String(message.activeSessionEpoch) : message.activeSessionEpoch;
                else
                    object.activeSessionEpoch = options.longs === $String ? $util.Long.prototype.toString.call(message.activeSessionEpoch) : options.longs === $Number ? new $util.LongBits(message.activeSessionEpoch.low >>> 0, message.activeSessionEpoch.high >>> 0).toNumber(true) : message.activeSessionEpoch;
            if (message.sessionAuthorityEpoch != null && $Object.hasOwnProperty.call(message, "sessionAuthorityEpoch"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.sessionAuthorityEpoch = typeof message.sessionAuthorityEpoch === "number" ? $BigInt(message.sessionAuthorityEpoch) : $util.Long.fromBits(message.sessionAuthorityEpoch.low >>> 0, message.sessionAuthorityEpoch.high >>> 0, true).toBigInt();
                else if (typeof message.sessionAuthorityEpoch === "number")
                    object.sessionAuthorityEpoch = options.longs === $String ? $String(message.sessionAuthorityEpoch) : message.sessionAuthorityEpoch;
                else
                    object.sessionAuthorityEpoch = options.longs === $String ? $util.Long.prototype.toString.call(message.sessionAuthorityEpoch) : options.longs === $Number ? new $util.LongBits(message.sessionAuthorityEpoch.low >>> 0, message.sessionAuthorityEpoch.high >>> 0).toNumber(true) : message.sessionAuthorityEpoch;
            if (message.serverTimeMs != null && $Object.hasOwnProperty.call(message, "serverTimeMs"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.serverTimeMs = typeof message.serverTimeMs === "number" ? $BigInt(message.serverTimeMs) : $util.Long.fromBits(message.serverTimeMs.low >>> 0, message.serverTimeMs.high >>> 0, false).toBigInt();
                else if (typeof message.serverTimeMs === "number")
                    object.serverTimeMs = options.longs === $String ? $String(message.serverTimeMs) : message.serverTimeMs;
                else
                    object.serverTimeMs = options.longs === $String ? $util.Long.prototype.toString.call(message.serverTimeMs) : options.longs === $Number ? new $util.LongBits(message.serverTimeMs.low >>> 0, message.serverTimeMs.high >>> 0).toNumber() : message.serverTimeMs;
            if (message.maxFrameBytes != null && $Object.hasOwnProperty.call(message, "maxFrameBytes"))
                object.maxFrameBytes = message.maxFrameBytes;
            return object;
        };

        /**
         * Converts this ServerHello to JSON.
         * @function toJSON
         * @memberof common.ServerHello
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        ServerHello.prototype.toJSON = function() {
            return ServerHello.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the type url for ServerHello
         * @function getTypeUrl
         * @memberof common.ServerHello
         * @static
         * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
         * @returns {string} The type url
         */
        ServerHello.getTypeUrl = function(prefix) {
            if (prefix === $undefined)
                prefix = "type.googleapis.com";
            return prefix + "/common.ServerHello";
        };

        return ServerHello;
    })();

    common.ConnectionError = (function() {

        /**
         * Properties of a ConnectionError.
         * @typedef {Object} common.ConnectionError.$Properties
         * @property {common.ErrorCode|null} [errorCode] ConnectionError errorCode
         * @property {string|null} [errorMessage] ConnectionError errorMessage
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */

        /**
         * Properties of a ConnectionError.
         * @memberof common
         * @interface IConnectionError
         * @augments common.ConnectionError.$Properties
         * @deprecated Use common.ConnectionError.$Properties instead.
         */

        /**
         * Shape of a ConnectionError.
         * @typedef {common.ConnectionError.$Properties} common.ConnectionError.$Shape
         */

        /**
         * Constructs a new ConnectionError.
         * @memberof common
         * @classdesc Represents a ConnectionError.
         * @constructor
         * @param {common.ConnectionError.$Properties=} [properties] Properties to set
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */
        var ConnectionError = function (properties) {
            if (properties)
                for (var keys = $Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null && keys[i] !== "__proto__")
                        this[keys[i]] = properties[keys[i]];
        };

        /**
         * ConnectionError errorCode.
         * @member {common.ErrorCode} errorCode
         * @memberof common.ConnectionError
         * @instance
         */
        ConnectionError.prototype.errorCode = 0;

        /**
         * ConnectionError errorMessage.
         * @member {string} errorMessage
         * @memberof common.ConnectionError
         * @instance
         */
        ConnectionError.prototype.errorMessage = "";

        /**
         * Creates a new ConnectionError instance using the specified properties.
         * @function create
         * @memberof common.ConnectionError
         * @static
         * @param {common.ConnectionError.$Properties=} [properties] Properties to set
         * @returns {common.ConnectionError} ConnectionError instance
         * @type {{
         *   (properties: common.ConnectionError.$Shape): common.ConnectionError & common.ConnectionError.$Shape;
         *   (properties?: common.ConnectionError.$Properties): common.ConnectionError;
         * }}
         */
        ConnectionError.create = function(properties) {
            return new ConnectionError(properties);
        };

        /**
         * Encodes the specified ConnectionError message. Does not implicitly {@link common.ConnectionError.verify|verify} messages.
         * @function encode
         * @memberof common.ConnectionError
         * @static
         * @param {common.ConnectionError.$Properties} message ConnectionError message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ConnectionError.encode = function (message, writer, _depth) {
            if (!writer)
                writer = $Writer.create();
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            if (message.errorCode != null && $Object.hasOwnProperty.call(message, "errorCode") && message.errorCode !== 0)
                writer.uint32(/* id 1, wireType 0 =*/8).int32(message.errorCode);
            if (message.errorMessage != null && $Object.hasOwnProperty.call(message, "errorMessage") && message.errorMessage !== "")
                writer.uint32(/* id 2, wireType 2 =*/18).string(message.errorMessage);
            if (message.$unknowns != null && $Object.hasOwnProperty.call(message, "$unknowns"))
                for (var i = 0; i < message.$unknowns.length; ++i)
                    writer.raw(message.$unknowns[i]);
            return writer;
        };

        /**
         * Encodes the specified ConnectionError message, length delimited. Does not implicitly {@link common.ConnectionError.verify|verify} messages.
         * @function encodeDelimited
         * @memberof common.ConnectionError
         * @static
         * @param {common.ConnectionError.$Properties} message ConnectionError message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ConnectionError.encodeDelimited = function(message, writer) {
            return this.encode(message, (writer || $Writer.create()).fork()).ldelim();
        };

        /**
         * Decodes a ConnectionError message from the specified reader or buffer.
         * @function decode
         * @memberof common.ConnectionError
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {common.ConnectionError & common.ConnectionError.$Shape} ConnectionError
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ConnectionError.decode = function (reader, length, _end, _depth, _target) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $Reader.recursionLimit)
                throw $Error("max depth exceeded");
            var end = length === $undefined ? reader.len : reader.pos + length, message = _target || new $root.common.ConnectionError(), value;
            while (reader.pos < end) {
                var start = reader.pos;
                var tag = reader.tag();
                if (tag === _end) {
                    _end = $undefined;
                    break;
                }
                var wireType = tag & 7;
                switch (tag >>>= 3) {
                case 1: {
                        if (wireType !== 0)
                            break;
                        if (value = reader.int32())
                            message.errorCode = value;
                        else
                            delete message.errorCode;
                        continue;
                    }
                case 2: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.errorMessage = value;
                        else
                            delete message.errorMessage;
                        continue;
                    }
                }
                reader.skipType(wireType, _depth, tag);
                if (!reader.discardUnknown) {
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
            }
            if (_end !== $undefined)
                throw $Error("missing end group");
            return message;
        };

        /**
         * Decodes a ConnectionError message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof common.ConnectionError
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {common.ConnectionError & common.ConnectionError.$Shape} ConnectionError
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ConnectionError.decodeDelimited = function(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a ConnectionError message.
         * @function verify
         * @memberof common.ConnectionError
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        ConnectionError.verify = function (message, _depth) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                return "max depth exceeded";
            if (message.errorCode != null && $Object.hasOwnProperty.call(message, "errorCode"))
                if (typeof message.errorCode !== "number" || (message.errorCode | 0) !== message.errorCode)
                    return "errorCode: enum value expected";
            if (message.errorMessage != null && $Object.hasOwnProperty.call(message, "errorMessage"))
                if (!$util.isString(message.errorMessage))
                    return "errorMessage: string expected";
            return null;
        };

        /**
         * Creates a ConnectionError message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof common.ConnectionError
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {common.ConnectionError} ConnectionError
         */
        ConnectionError.fromObject = function (object, _depth) {
            if (object instanceof $root.common.ConnectionError)
                return object;
            if (!$util.isObject(object))
                throw $TypeError(".common.ConnectionError: object expected");
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var message = new $root.common.ConnectionError();
            if (object.errorCode !== 0 && (typeof object.errorCode !== "string" || $root.common.ErrorCode[object.errorCode] !== 0))
                switch (object.errorCode) {
                case "ERROR_CODE_UNSPECIFIED":
                case 0:
                    message.errorCode = 0;
                    break;
                case "OK":
                case 1:
                    message.errorCode = 1;
                    break;
                case "ERR_BAD_FRAME":
                case 1001:
                    message.errorCode = 1001;
                    break;
                case "ERR_DECODE":
                case 1002:
                    message.errorCode = 1002;
                    break;
                case "ERR_UNSUPPORTED_VERSION":
                case 1003:
                    message.errorCode = 1003;
                    break;
                case "ERR_MESSAGE_TOO_LARGE":
                case 1004:
                    message.errorCode = 1004;
                    break;
                case "ERR_UNKNOWN_COMMAND":
                case 1005:
                    message.errorCode = 1005;
                    break;
                case "ERR_RATE_LIMITED":
                case 1006:
                    message.errorCode = 1006;
                    break;
                case "ERR_UNAUTHENTICATED":
                case 2001:
                    message.errorCode = 2001;
                    break;
                case "ERR_SESSION_EXPIRED":
                case 2002:
                    message.errorCode = 2002;
                    break;
                case "ERR_SESSION_PERMIT_INVALID":
                case 2003:
                    message.errorCode = 2003;
                    break;
                case "ERR_LOGIN_ELSEWHERE":
                case 2004:
                    message.errorCode = 2004;
                    break;
                case "ERR_CONNECTION_TICKET_INVALID":
                case 2005:
                    message.errorCode = 2005;
                    break;
                case "ERR_NOT_FRIEND":
                case 3001:
                    message.errorCode = 3001;
                    break;
                case "ERR_FRIEND_PERMIT_INVALID":
                case 3002:
                    message.errorCode = 3002;
                    break;
                case "ERR_FRIEND_LIMIT":
                case 3003:
                    message.errorCode = 3003;
                    break;
                case "ERR_SHARE_LINK_INVALID":
                case 3004:
                    message.errorCode = 3004;
                    break;
                case "ERR_PERMISSION_DENIED":
                case 3005:
                    message.errorCode = 3005;
                    break;
                case "ERR_INSUFFICIENT_COIN":
                case 4001:
                    message.errorCode = 4001;
                    break;
                case "ERR_INSUFFICIENT_ITEM":
                case 4002:
                    message.errorCode = 4002;
                    break;
                case "ERR_OFFER_NOT_FOUND":
                case 4003:
                    message.errorCode = 4003;
                    break;
                case "ERR_RULE_VERSION_MISMATCH":
                case 4004:
                    message.errorCode = 4004;
                    break;
                case "ERR_PLOT_NOT_EMPTY":
                case 5001:
                    message.errorCode = 5001;
                    break;
                case "ERR_PLOT_EMPTY":
                case 5002:
                    message.errorCode = 5002;
                    break;
                case "ERR_CROP_NOT_MATURE":
                case 5003:
                    message.errorCode = 5003;
                    break;
                case "ERR_VERSION_CONFLICT":
                case 5004:
                    message.errorCode = 5004;
                    break;
                case "ERR_PLOT_NOT_FOUND":
                case 5005:
                    message.errorCode = 5005;
                    break;
                case "ERR_FARM_FULL":
                case 6001:
                    message.errorCode = 6001;
                    break;
                case "ERR_NOT_IN_FARM":
                case 6002:
                    message.errorCode = 6002;
                    break;
                case "ERR_ACTOR_REDIRECT":
                case 6003:
                    message.errorCode = 6003;
                    break;
                case "ERR_DOWNLINK_OVERFLOW":
                case 6004:
                    message.errorCode = 6004;
                    break;
                case "ERR_STEAL_ALREADY_DONE":
                case 7001:
                    message.errorCode = 7001;
                    break;
                case "ERR_STEAL_QUOTA_EXHAUSTED":
                case 7002:
                    message.errorCode = 7002;
                    break;
                case "ERR_STEAL_BLOCKED_BY_PET":
                case 7003:
                    message.errorCode = 7003;
                    break;
                case "ERR_SETTLEMENT_NOT_FOUND":
                case 7004:
                    message.errorCode = 7004;
                    break;
                case "ERR_INTERNAL":
                case 9001:
                    message.errorCode = 9001;
                    break;
                case "ERR_STORE_CAS_CONFLICT":
                case 9002:
                    message.errorCode = 9002;
                    break;
                case "ERR_STORE_UNAVAILABLE":
                case 9003:
                    message.errorCode = 9003;
                    break;
                case "ERR_TIMEOUT":
                case 9004:
                    message.errorCode = 9004;
                    break;
                default:
                    if (typeof object.errorCode === "number" && (object.errorCode | 0) === object.errorCode)
                        message.errorCode = object.errorCode;
                }
            if (object.errorMessage != null)
                if (typeof object.errorMessage !== "string" || object.errorMessage.length)
                    message.errorMessage = $String(object.errorMessage);
            return message;
        };

        /**
         * Creates a plain object from a ConnectionError message. Also converts values to other types if specified.
         * @function toObject
         * @memberof common.ConnectionError
         * @static
         * @param {common.ConnectionError} message ConnectionError
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        ConnectionError.toObject = function (message, options, _depth) {
            if (!options)
                options = {};
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var object = {};
            if (options.defaults) {
                object.errorCode = options.enums === $String ? "ERROR_CODE_UNSPECIFIED" : 0;
                object.errorMessage = "";
            }
            if (message.errorCode != null && $Object.hasOwnProperty.call(message, "errorCode"))
                object.errorCode = options.enums === $String ? $root.common.ErrorCode[message.errorCode] === $undefined ? message.errorCode : $root.common.ErrorCode[message.errorCode] : message.errorCode;
            if (message.errorMessage != null && $Object.hasOwnProperty.call(message, "errorMessage"))
                object.errorMessage = message.errorMessage;
            return object;
        };

        /**
         * Converts this ConnectionError to JSON.
         * @function toJSON
         * @memberof common.ConnectionError
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        ConnectionError.prototype.toJSON = function() {
            return ConnectionError.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the type url for ConnectionError
         * @function getTypeUrl
         * @memberof common.ConnectionError
         * @static
         * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
         * @returns {string} The type url
         */
        ConnectionError.getTypeUrl = function(prefix) {
            if (prefix === $undefined)
                prefix = "type.googleapis.com";
            return prefix + "/common.ConnectionError";
        };

        return ConnectionError;
    })();

    /**
     * CommandType enum.
     * @name common.CommandType
     * @enum {number}
     * @property {number} COMMAND_TYPE_UNSPECIFIED=0 COMMAND_TYPE_UNSPECIFIED value
     * @property {number} BUY_OFFER=2 BUY_OFFER value
     * @property {number} SELL_ITEM=3 SELL_ITEM value
     * @property {number} PLANT_CROP=4 PLANT_CROP value
     * @property {number} HARVEST_CROP=5 HARVEST_CROP value
     * @property {number} JOIN_FARM=6 JOIN_FARM value
     * @property {number} LEAVE_FARM=7 LEAVE_FARM value
     * @property {number} WATER_CROP=8 WATER_CROP value
     * @property {number} BEGIN_THEFT=9 BEGIN_THEFT value
     * @property {number} GET_FARM_SNAPSHOT=10 GET_FARM_SNAPSHOT value
     * @property {number} GET_OPERATION_RESULT=11 GET_OPERATION_RESULT value
     */
    common.CommandType = (function() {
        var valuesById = $Object.create(null), values = $Object.create(valuesById);
        values[valuesById[0] = "COMMAND_TYPE_UNSPECIFIED"] = 0;
        values[valuesById[2] = "BUY_OFFER"] = 2;
        values[valuesById[3] = "SELL_ITEM"] = 3;
        values[valuesById[4] = "PLANT_CROP"] = 4;
        values[valuesById[5] = "HARVEST_CROP"] = 5;
        values[valuesById[6] = "JOIN_FARM"] = 6;
        values[valuesById[7] = "LEAVE_FARM"] = 7;
        values[valuesById[8] = "WATER_CROP"] = 8;
        values[valuesById[9] = "BEGIN_THEFT"] = 9;
        values[valuesById[10] = "GET_FARM_SNAPSHOT"] = 10;
        values[valuesById[11] = "GET_OPERATION_RESULT"] = 11;
        return values;
    })();

    common.ClientCommandEnvelope = (function() {

        /**
         * Properties of a ClientCommandEnvelope.
         * @typedef {Object} common.ClientCommandEnvelope.$Properties
         * @property {string|null} [operationId] ClientCommandEnvelope operationId
         * @property {common.CommandType|null} [commandType] ClientCommandEnvelope commandType
         * @property {number|Long|null} [targetOwnerId] ClientCommandEnvelope targetOwnerId
         * @property {number|Long|null} [expectedFarmVersion] ClientCommandEnvelope expectedFarmVersion
         * @property {number|Long|null} [expectedPlotVersion] ClientCommandEnvelope expectedPlotVersion
         * @property {string|null} [expectedCropCycleId] ClientCommandEnvelope expectedCropCycleId
         * @property {string|null} [requestId] ClientCommandEnvelope requestId
         * @property {Uint8Array|null} [payload] ClientCommandEnvelope payload
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */

        /**
         * Properties of a ClientCommandEnvelope.
         * @memberof common
         * @interface IClientCommandEnvelope
         * @augments common.ClientCommandEnvelope.$Properties
         * @deprecated Use common.ClientCommandEnvelope.$Properties instead.
         */

        /**
         * Shape of a ClientCommandEnvelope.
         * @typedef {common.ClientCommandEnvelope.$Properties} common.ClientCommandEnvelope.$Shape
         */

        /**
         * Constructs a new ClientCommandEnvelope.
         * @memberof common
         * @classdesc Represents a ClientCommandEnvelope.
         * @constructor
         * @param {common.ClientCommandEnvelope.$Properties=} [properties] Properties to set
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */
        var ClientCommandEnvelope = function (properties) {
            if (properties)
                for (var keys = $Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null && keys[i] !== "__proto__")
                        this[keys[i]] = properties[keys[i]];
        };

        /**
         * ClientCommandEnvelope operationId.
         * @member {string} operationId
         * @memberof common.ClientCommandEnvelope
         * @instance
         */
        ClientCommandEnvelope.prototype.operationId = "";

        /**
         * ClientCommandEnvelope commandType.
         * @member {common.CommandType} commandType
         * @memberof common.ClientCommandEnvelope
         * @instance
         */
        ClientCommandEnvelope.prototype.commandType = 0;

        /**
         * ClientCommandEnvelope targetOwnerId.
         * @member {number|Long} targetOwnerId
         * @memberof common.ClientCommandEnvelope
         * @instance
         */
        ClientCommandEnvelope.prototype.targetOwnerId = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * ClientCommandEnvelope expectedFarmVersion.
         * @member {number|Long} expectedFarmVersion
         * @memberof common.ClientCommandEnvelope
         * @instance
         */
        ClientCommandEnvelope.prototype.expectedFarmVersion = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * ClientCommandEnvelope expectedPlotVersion.
         * @member {number|Long} expectedPlotVersion
         * @memberof common.ClientCommandEnvelope
         * @instance
         */
        ClientCommandEnvelope.prototype.expectedPlotVersion = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * ClientCommandEnvelope expectedCropCycleId.
         * @member {string} expectedCropCycleId
         * @memberof common.ClientCommandEnvelope
         * @instance
         */
        ClientCommandEnvelope.prototype.expectedCropCycleId = "";

        /**
         * ClientCommandEnvelope requestId.
         * @member {string} requestId
         * @memberof common.ClientCommandEnvelope
         * @instance
         */
        ClientCommandEnvelope.prototype.requestId = "";

        /**
         * ClientCommandEnvelope payload.
         * @member {Uint8Array} payload
         * @memberof common.ClientCommandEnvelope
         * @instance
         */
        ClientCommandEnvelope.prototype.payload = $util.newBuffer([]);

        /**
         * Creates a new ClientCommandEnvelope instance using the specified properties.
         * @function create
         * @memberof common.ClientCommandEnvelope
         * @static
         * @param {common.ClientCommandEnvelope.$Properties=} [properties] Properties to set
         * @returns {common.ClientCommandEnvelope} ClientCommandEnvelope instance
         * @type {{
         *   (properties: common.ClientCommandEnvelope.$Shape): common.ClientCommandEnvelope & common.ClientCommandEnvelope.$Shape;
         *   (properties?: common.ClientCommandEnvelope.$Properties): common.ClientCommandEnvelope;
         * }}
         */
        ClientCommandEnvelope.create = function(properties) {
            return new ClientCommandEnvelope(properties);
        };

        /**
         * Encodes the specified ClientCommandEnvelope message. Does not implicitly {@link common.ClientCommandEnvelope.verify|verify} messages.
         * @function encode
         * @memberof common.ClientCommandEnvelope
         * @static
         * @param {common.ClientCommandEnvelope.$Properties} message ClientCommandEnvelope message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ClientCommandEnvelope.encode = function (message, writer, _depth) {
            if (!writer)
                writer = $Writer.create();
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            if (message.operationId != null && $Object.hasOwnProperty.call(message, "operationId") && message.operationId !== "")
                writer.uint32(/* id 2, wireType 2 =*/18).string(message.operationId);
            if (message.commandType != null && $Object.hasOwnProperty.call(message, "commandType") && message.commandType !== 0)
                writer.uint32(/* id 4, wireType 0 =*/32).int32(message.commandType);
            if (message.targetOwnerId != null && $Object.hasOwnProperty.call(message, "targetOwnerId") && (typeof message.targetOwnerId === "object" ? message.targetOwnerId.low || message.targetOwnerId.high : message.targetOwnerId !== 0))
                writer.uint32(/* id 5, wireType 0 =*/40).uint64(message.targetOwnerId);
            if (message.expectedFarmVersion != null && $Object.hasOwnProperty.call(message, "expectedFarmVersion") && (typeof message.expectedFarmVersion === "object" ? message.expectedFarmVersion.low || message.expectedFarmVersion.high : message.expectedFarmVersion !== 0))
                writer.uint32(/* id 6, wireType 0 =*/48).uint64(message.expectedFarmVersion);
            if (message.expectedPlotVersion != null && $Object.hasOwnProperty.call(message, "expectedPlotVersion") && (typeof message.expectedPlotVersion === "object" ? message.expectedPlotVersion.low || message.expectedPlotVersion.high : message.expectedPlotVersion !== 0))
                writer.uint32(/* id 7, wireType 0 =*/56).uint64(message.expectedPlotVersion);
            if (message.expectedCropCycleId != null && $Object.hasOwnProperty.call(message, "expectedCropCycleId") && message.expectedCropCycleId !== "")
                writer.uint32(/* id 8, wireType 2 =*/66).string(message.expectedCropCycleId);
            if (message.requestId != null && $Object.hasOwnProperty.call(message, "requestId") && message.requestId !== "")
                writer.uint32(/* id 10, wireType 2 =*/82).string(message.requestId);
            if (message.payload != null && $Object.hasOwnProperty.call(message, "payload") && message.payload.length)
                writer.uint32(/* id 15, wireType 2 =*/122).bytes(message.payload);
            if (message.$unknowns != null && $Object.hasOwnProperty.call(message, "$unknowns"))
                for (var i = 0; i < message.$unknowns.length; ++i)
                    writer.raw(message.$unknowns[i]);
            return writer;
        };

        /**
         * Encodes the specified ClientCommandEnvelope message, length delimited. Does not implicitly {@link common.ClientCommandEnvelope.verify|verify} messages.
         * @function encodeDelimited
         * @memberof common.ClientCommandEnvelope
         * @static
         * @param {common.ClientCommandEnvelope.$Properties} message ClientCommandEnvelope message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ClientCommandEnvelope.encodeDelimited = function(message, writer) {
            return this.encode(message, (writer || $Writer.create()).fork()).ldelim();
        };

        /**
         * Decodes a ClientCommandEnvelope message from the specified reader or buffer.
         * @function decode
         * @memberof common.ClientCommandEnvelope
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {common.ClientCommandEnvelope & common.ClientCommandEnvelope.$Shape} ClientCommandEnvelope
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ClientCommandEnvelope.decode = function (reader, length, _end, _depth, _target) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $Reader.recursionLimit)
                throw $Error("max depth exceeded");
            var end = length === $undefined ? reader.len : reader.pos + length, message = _target || new $root.common.ClientCommandEnvelope(), value;
            while (reader.pos < end) {
                var start = reader.pos;
                var tag = reader.tag();
                if (tag === _end) {
                    _end = $undefined;
                    break;
                }
                var wireType = tag & 7;
                switch (tag >>>= 3) {
                case 2: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.operationId = value;
                        else
                            delete message.operationId;
                        continue;
                    }
                case 4: {
                        if (wireType !== 0)
                            break;
                        if (value = reader.int32())
                            message.commandType = value;
                        else
                            delete message.commandType;
                        continue;
                    }
                case 5: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.targetOwnerId = value;
                        else
                            delete message.targetOwnerId;
                        continue;
                    }
                case 6: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.expectedFarmVersion = value;
                        else
                            delete message.expectedFarmVersion;
                        continue;
                    }
                case 7: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.expectedPlotVersion = value;
                        else
                            delete message.expectedPlotVersion;
                        continue;
                    }
                case 8: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.expectedCropCycleId = value;
                        else
                            delete message.expectedCropCycleId;
                        continue;
                    }
                case 10: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.requestId = value;
                        else
                            delete message.requestId;
                        continue;
                    }
                case 15: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.bytes()).length)
                            message.payload = value;
                        else
                            delete message.payload;
                        continue;
                    }
                }
                reader.skipType(wireType, _depth, tag);
                if (!reader.discardUnknown) {
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
            }
            if (_end !== $undefined)
                throw $Error("missing end group");
            return message;
        };

        /**
         * Decodes a ClientCommandEnvelope message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof common.ClientCommandEnvelope
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {common.ClientCommandEnvelope & common.ClientCommandEnvelope.$Shape} ClientCommandEnvelope
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ClientCommandEnvelope.decodeDelimited = function(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a ClientCommandEnvelope message.
         * @function verify
         * @memberof common.ClientCommandEnvelope
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        ClientCommandEnvelope.verify = function (message, _depth) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                return "max depth exceeded";
            if (message.operationId != null && $Object.hasOwnProperty.call(message, "operationId"))
                if (!$util.isString(message.operationId))
                    return "operationId: string expected";
            if (message.commandType != null && $Object.hasOwnProperty.call(message, "commandType"))
                if (typeof message.commandType !== "number" || (message.commandType | 0) !== message.commandType)
                    return "commandType: enum value expected";
            if (message.targetOwnerId != null && $Object.hasOwnProperty.call(message, "targetOwnerId"))
                if (!$util.isInteger(message.targetOwnerId) && !(message.targetOwnerId && $util.isInteger(message.targetOwnerId.low) && $util.isInteger(message.targetOwnerId.high)))
                    return "targetOwnerId: integer|Long expected";
            if (message.expectedFarmVersion != null && $Object.hasOwnProperty.call(message, "expectedFarmVersion"))
                if (!$util.isInteger(message.expectedFarmVersion) && !(message.expectedFarmVersion && $util.isInteger(message.expectedFarmVersion.low) && $util.isInteger(message.expectedFarmVersion.high)))
                    return "expectedFarmVersion: integer|Long expected";
            if (message.expectedPlotVersion != null && $Object.hasOwnProperty.call(message, "expectedPlotVersion"))
                if (!$util.isInteger(message.expectedPlotVersion) && !(message.expectedPlotVersion && $util.isInteger(message.expectedPlotVersion.low) && $util.isInteger(message.expectedPlotVersion.high)))
                    return "expectedPlotVersion: integer|Long expected";
            if (message.expectedCropCycleId != null && $Object.hasOwnProperty.call(message, "expectedCropCycleId"))
                if (!$util.isString(message.expectedCropCycleId))
                    return "expectedCropCycleId: string expected";
            if (message.requestId != null && $Object.hasOwnProperty.call(message, "requestId"))
                if (!$util.isString(message.requestId))
                    return "requestId: string expected";
            if (message.payload != null && $Object.hasOwnProperty.call(message, "payload"))
                if (!(message.payload && typeof message.payload.length === "number" || $util.isString(message.payload)))
                    return "payload: buffer expected";
            return null;
        };

        /**
         * Creates a ClientCommandEnvelope message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof common.ClientCommandEnvelope
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {common.ClientCommandEnvelope} ClientCommandEnvelope
         */
        ClientCommandEnvelope.fromObject = function (object, _depth) {
            if (object instanceof $root.common.ClientCommandEnvelope)
                return object;
            if (!$util.isObject(object))
                throw $TypeError(".common.ClientCommandEnvelope: object expected");
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var message = new $root.common.ClientCommandEnvelope();
            if (object.operationId != null)
                if (typeof object.operationId !== "string" || object.operationId.length)
                    message.operationId = $String(object.operationId);
            if (object.commandType !== 0 && (typeof object.commandType !== "string" || $root.common.CommandType[object.commandType] !== 0))
                switch (object.commandType) {
                case "COMMAND_TYPE_UNSPECIFIED":
                case 0:
                    message.commandType = 0;
                    break;
                case "BUY_OFFER":
                case 2:
                    message.commandType = 2;
                    break;
                case "SELL_ITEM":
                case 3:
                    message.commandType = 3;
                    break;
                case "PLANT_CROP":
                case 4:
                    message.commandType = 4;
                    break;
                case "HARVEST_CROP":
                case 5:
                    message.commandType = 5;
                    break;
                case "JOIN_FARM":
                case 6:
                    message.commandType = 6;
                    break;
                case "LEAVE_FARM":
                case 7:
                    message.commandType = 7;
                    break;
                case "WATER_CROP":
                case 8:
                    message.commandType = 8;
                    break;
                case "BEGIN_THEFT":
                case 9:
                    message.commandType = 9;
                    break;
                case "GET_FARM_SNAPSHOT":
                case 10:
                    message.commandType = 10;
                    break;
                case "GET_OPERATION_RESULT":
                case 11:
                    message.commandType = 11;
                    break;
                default:
                    if (typeof object.commandType === "number" && (object.commandType | 0) === object.commandType)
                        message.commandType = object.commandType;
                }
            if (object.targetOwnerId != null)
                if (typeof object.targetOwnerId === "object" ? object.targetOwnerId.low || object.targetOwnerId.high : $Number(object.targetOwnerId) !== 0)
                    if ($util.Long)
                        message.targetOwnerId = $util.Long.fromValue(object.targetOwnerId, true);
                    else if (typeof object.targetOwnerId === "string")
                        message.targetOwnerId = $parseInt(object.targetOwnerId, 10);
                    else if (typeof object.targetOwnerId === "number")
                        message.targetOwnerId = object.targetOwnerId;
                    else if (typeof object.targetOwnerId === "object")
                        message.targetOwnerId = new $util.LongBits(object.targetOwnerId.low >>> 0, object.targetOwnerId.high >>> 0).toNumber(true);
            if (object.expectedFarmVersion != null)
                if (typeof object.expectedFarmVersion === "object" ? object.expectedFarmVersion.low || object.expectedFarmVersion.high : $Number(object.expectedFarmVersion) !== 0)
                    if ($util.Long)
                        message.expectedFarmVersion = $util.Long.fromValue(object.expectedFarmVersion, true);
                    else if (typeof object.expectedFarmVersion === "string")
                        message.expectedFarmVersion = $parseInt(object.expectedFarmVersion, 10);
                    else if (typeof object.expectedFarmVersion === "number")
                        message.expectedFarmVersion = object.expectedFarmVersion;
                    else if (typeof object.expectedFarmVersion === "object")
                        message.expectedFarmVersion = new $util.LongBits(object.expectedFarmVersion.low >>> 0, object.expectedFarmVersion.high >>> 0).toNumber(true);
            if (object.expectedPlotVersion != null)
                if (typeof object.expectedPlotVersion === "object" ? object.expectedPlotVersion.low || object.expectedPlotVersion.high : $Number(object.expectedPlotVersion) !== 0)
                    if ($util.Long)
                        message.expectedPlotVersion = $util.Long.fromValue(object.expectedPlotVersion, true);
                    else if (typeof object.expectedPlotVersion === "string")
                        message.expectedPlotVersion = $parseInt(object.expectedPlotVersion, 10);
                    else if (typeof object.expectedPlotVersion === "number")
                        message.expectedPlotVersion = object.expectedPlotVersion;
                    else if (typeof object.expectedPlotVersion === "object")
                        message.expectedPlotVersion = new $util.LongBits(object.expectedPlotVersion.low >>> 0, object.expectedPlotVersion.high >>> 0).toNumber(true);
            if (object.expectedCropCycleId != null)
                if (typeof object.expectedCropCycleId !== "string" || object.expectedCropCycleId.length)
                    message.expectedCropCycleId = $String(object.expectedCropCycleId);
            if (object.requestId != null)
                if (typeof object.requestId !== "string" || object.requestId.length)
                    message.requestId = $String(object.requestId);
            if (object.payload != null)
                if (object.payload.length)
                    if (typeof object.payload === "string")
                        $util.base64.decode(object.payload, message.payload = $util.newBuffer($util.base64.length(object.payload)), 0);
                    else if (object.payload.length >= 0)
                        message.payload = object.payload;
            return message;
        };

        /**
         * Creates a plain object from a ClientCommandEnvelope message. Also converts values to other types if specified.
         * @function toObject
         * @memberof common.ClientCommandEnvelope
         * @static
         * @param {common.ClientCommandEnvelope} message ClientCommandEnvelope
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        ClientCommandEnvelope.toObject = function (message, options, _depth) {
            if (!options)
                options = {};
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var object = {};
            if (options.defaults) {
                object.operationId = "";
                object.commandType = options.enums === $String ? "COMMAND_TYPE_UNSPECIFIED" : 0;
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.targetOwnerId = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.targetOwnerId = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.expectedFarmVersion = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.expectedFarmVersion = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.expectedPlotVersion = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.expectedPlotVersion = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                object.expectedCropCycleId = "";
                object.requestId = "";
                if (options.bytes === $String)
                    object.payload = "";
                else {
                    object.payload = [];
                    if (options.bytes !== $Array)
                        object.payload = $util.newBuffer(object.payload);
                }
            }
            if (message.operationId != null && $Object.hasOwnProperty.call(message, "operationId"))
                object.operationId = message.operationId;
            if (message.commandType != null && $Object.hasOwnProperty.call(message, "commandType"))
                object.commandType = options.enums === $String ? $root.common.CommandType[message.commandType] === $undefined ? message.commandType : $root.common.CommandType[message.commandType] : message.commandType;
            if (message.targetOwnerId != null && $Object.hasOwnProperty.call(message, "targetOwnerId"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.targetOwnerId = typeof message.targetOwnerId === "number" ? $BigInt(message.targetOwnerId) : $util.Long.fromBits(message.targetOwnerId.low >>> 0, message.targetOwnerId.high >>> 0, true).toBigInt();
                else if (typeof message.targetOwnerId === "number")
                    object.targetOwnerId = options.longs === $String ? $String(message.targetOwnerId) : message.targetOwnerId;
                else
                    object.targetOwnerId = options.longs === $String ? $util.Long.prototype.toString.call(message.targetOwnerId) : options.longs === $Number ? new $util.LongBits(message.targetOwnerId.low >>> 0, message.targetOwnerId.high >>> 0).toNumber(true) : message.targetOwnerId;
            if (message.expectedFarmVersion != null && $Object.hasOwnProperty.call(message, "expectedFarmVersion"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.expectedFarmVersion = typeof message.expectedFarmVersion === "number" ? $BigInt(message.expectedFarmVersion) : $util.Long.fromBits(message.expectedFarmVersion.low >>> 0, message.expectedFarmVersion.high >>> 0, true).toBigInt();
                else if (typeof message.expectedFarmVersion === "number")
                    object.expectedFarmVersion = options.longs === $String ? $String(message.expectedFarmVersion) : message.expectedFarmVersion;
                else
                    object.expectedFarmVersion = options.longs === $String ? $util.Long.prototype.toString.call(message.expectedFarmVersion) : options.longs === $Number ? new $util.LongBits(message.expectedFarmVersion.low >>> 0, message.expectedFarmVersion.high >>> 0).toNumber(true) : message.expectedFarmVersion;
            if (message.expectedPlotVersion != null && $Object.hasOwnProperty.call(message, "expectedPlotVersion"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.expectedPlotVersion = typeof message.expectedPlotVersion === "number" ? $BigInt(message.expectedPlotVersion) : $util.Long.fromBits(message.expectedPlotVersion.low >>> 0, message.expectedPlotVersion.high >>> 0, true).toBigInt();
                else if (typeof message.expectedPlotVersion === "number")
                    object.expectedPlotVersion = options.longs === $String ? $String(message.expectedPlotVersion) : message.expectedPlotVersion;
                else
                    object.expectedPlotVersion = options.longs === $String ? $util.Long.prototype.toString.call(message.expectedPlotVersion) : options.longs === $Number ? new $util.LongBits(message.expectedPlotVersion.low >>> 0, message.expectedPlotVersion.high >>> 0).toNumber(true) : message.expectedPlotVersion;
            if (message.expectedCropCycleId != null && $Object.hasOwnProperty.call(message, "expectedCropCycleId"))
                object.expectedCropCycleId = message.expectedCropCycleId;
            if (message.requestId != null && $Object.hasOwnProperty.call(message, "requestId"))
                object.requestId = message.requestId;
            if (message.payload != null && $Object.hasOwnProperty.call(message, "payload"))
                object.payload = options.bytes === $String ? $util.base64.encode(message.payload, 0, message.payload.length) : options.bytes === $Array ? $Array.prototype.slice.call(message.payload) : message.payload;
            return object;
        };

        /**
         * Converts this ClientCommandEnvelope to JSON.
         * @function toJSON
         * @memberof common.ClientCommandEnvelope
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        ClientCommandEnvelope.prototype.toJSON = function() {
            return ClientCommandEnvelope.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the type url for ClientCommandEnvelope
         * @function getTypeUrl
         * @memberof common.ClientCommandEnvelope
         * @static
         * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
         * @returns {string} The type url
         */
        ClientCommandEnvelope.getTypeUrl = function(prefix) {
            if (prefix === $undefined)
                prefix = "type.googleapis.com";
            return prefix + "/common.ClientCommandEnvelope";
        };

        return ClientCommandEnvelope;
    })();

    common.CommandResultEnvelope = (function() {

        /**
         * Properties of a CommandResultEnvelope.
         * @typedef {Object} common.CommandResultEnvelope.$Properties
         * @property {string|null} [operationId] CommandResultEnvelope operationId
         * @property {common.CommandStatus|null} [status] CommandResultEnvelope status
         * @property {common.ErrorCode|null} [errorCode] CommandResultEnvelope errorCode
         * @property {string|null} [errorMessage] CommandResultEnvelope errorMessage
         * @property {number|Long|null} [farmVersion] CommandResultEnvelope farmVersion
         * @property {string|null} [requestId] CommandResultEnvelope requestId
         * @property {Uint8Array|null} [payload] CommandResultEnvelope payload
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */

        /**
         * Properties of a CommandResultEnvelope.
         * @memberof common
         * @interface ICommandResultEnvelope
         * @augments common.CommandResultEnvelope.$Properties
         * @deprecated Use common.CommandResultEnvelope.$Properties instead.
         */

        /**
         * Shape of a CommandResultEnvelope.
         * @typedef {common.CommandResultEnvelope.$Properties} common.CommandResultEnvelope.$Shape
         */

        /**
         * Constructs a new CommandResultEnvelope.
         * @memberof common
         * @classdesc Represents a CommandResultEnvelope.
         * @constructor
         * @param {common.CommandResultEnvelope.$Properties=} [properties] Properties to set
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */
        var CommandResultEnvelope = function (properties) {
            if (properties)
                for (var keys = $Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null && keys[i] !== "__proto__")
                        this[keys[i]] = properties[keys[i]];
        };

        /**
         * CommandResultEnvelope operationId.
         * @member {string} operationId
         * @memberof common.CommandResultEnvelope
         * @instance
         */
        CommandResultEnvelope.prototype.operationId = "";

        /**
         * CommandResultEnvelope status.
         * @member {common.CommandStatus} status
         * @memberof common.CommandResultEnvelope
         * @instance
         */
        CommandResultEnvelope.prototype.status = 0;

        /**
         * CommandResultEnvelope errorCode.
         * @member {common.ErrorCode} errorCode
         * @memberof common.CommandResultEnvelope
         * @instance
         */
        CommandResultEnvelope.prototype.errorCode = 0;

        /**
         * CommandResultEnvelope errorMessage.
         * @member {string} errorMessage
         * @memberof common.CommandResultEnvelope
         * @instance
         */
        CommandResultEnvelope.prototype.errorMessage = "";

        /**
         * CommandResultEnvelope farmVersion.
         * @member {number|Long} farmVersion
         * @memberof common.CommandResultEnvelope
         * @instance
         */
        CommandResultEnvelope.prototype.farmVersion = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * CommandResultEnvelope requestId.
         * @member {string} requestId
         * @memberof common.CommandResultEnvelope
         * @instance
         */
        CommandResultEnvelope.prototype.requestId = "";

        /**
         * CommandResultEnvelope payload.
         * @member {Uint8Array} payload
         * @memberof common.CommandResultEnvelope
         * @instance
         */
        CommandResultEnvelope.prototype.payload = $util.newBuffer([]);

        /**
         * Creates a new CommandResultEnvelope instance using the specified properties.
         * @function create
         * @memberof common.CommandResultEnvelope
         * @static
         * @param {common.CommandResultEnvelope.$Properties=} [properties] Properties to set
         * @returns {common.CommandResultEnvelope} CommandResultEnvelope instance
         * @type {{
         *   (properties: common.CommandResultEnvelope.$Shape): common.CommandResultEnvelope & common.CommandResultEnvelope.$Shape;
         *   (properties?: common.CommandResultEnvelope.$Properties): common.CommandResultEnvelope;
         * }}
         */
        CommandResultEnvelope.create = function(properties) {
            return new CommandResultEnvelope(properties);
        };

        /**
         * Encodes the specified CommandResultEnvelope message. Does not implicitly {@link common.CommandResultEnvelope.verify|verify} messages.
         * @function encode
         * @memberof common.CommandResultEnvelope
         * @static
         * @param {common.CommandResultEnvelope.$Properties} message CommandResultEnvelope message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        CommandResultEnvelope.encode = function (message, writer, _depth) {
            if (!writer)
                writer = $Writer.create();
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            if (message.operationId != null && $Object.hasOwnProperty.call(message, "operationId") && message.operationId !== "")
                writer.uint32(/* id 1, wireType 2 =*/10).string(message.operationId);
            if (message.status != null && $Object.hasOwnProperty.call(message, "status") && message.status !== 0)
                writer.uint32(/* id 2, wireType 0 =*/16).int32(message.status);
            if (message.errorCode != null && $Object.hasOwnProperty.call(message, "errorCode") && message.errorCode !== 0)
                writer.uint32(/* id 3, wireType 0 =*/24).int32(message.errorCode);
            if (message.errorMessage != null && $Object.hasOwnProperty.call(message, "errorMessage") && message.errorMessage !== "")
                writer.uint32(/* id 4, wireType 2 =*/34).string(message.errorMessage);
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion") && (typeof message.farmVersion === "object" ? message.farmVersion.low || message.farmVersion.high : message.farmVersion !== 0))
                writer.uint32(/* id 6, wireType 0 =*/48).uint64(message.farmVersion);
            if (message.requestId != null && $Object.hasOwnProperty.call(message, "requestId") && message.requestId !== "")
                writer.uint32(/* id 7, wireType 2 =*/58).string(message.requestId);
            if (message.payload != null && $Object.hasOwnProperty.call(message, "payload") && message.payload.length)
                writer.uint32(/* id 15, wireType 2 =*/122).bytes(message.payload);
            if (message.$unknowns != null && $Object.hasOwnProperty.call(message, "$unknowns"))
                for (var i = 0; i < message.$unknowns.length; ++i)
                    writer.raw(message.$unknowns[i]);
            return writer;
        };

        /**
         * Encodes the specified CommandResultEnvelope message, length delimited. Does not implicitly {@link common.CommandResultEnvelope.verify|verify} messages.
         * @function encodeDelimited
         * @memberof common.CommandResultEnvelope
         * @static
         * @param {common.CommandResultEnvelope.$Properties} message CommandResultEnvelope message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        CommandResultEnvelope.encodeDelimited = function(message, writer) {
            return this.encode(message, (writer || $Writer.create()).fork()).ldelim();
        };

        /**
         * Decodes a CommandResultEnvelope message from the specified reader or buffer.
         * @function decode
         * @memberof common.CommandResultEnvelope
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {common.CommandResultEnvelope & common.CommandResultEnvelope.$Shape} CommandResultEnvelope
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        CommandResultEnvelope.decode = function (reader, length, _end, _depth, _target) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $Reader.recursionLimit)
                throw $Error("max depth exceeded");
            var end = length === $undefined ? reader.len : reader.pos + length, message = _target || new $root.common.CommandResultEnvelope(), value;
            while (reader.pos < end) {
                var start = reader.pos;
                var tag = reader.tag();
                if (tag === _end) {
                    _end = $undefined;
                    break;
                }
                var wireType = tag & 7;
                switch (tag >>>= 3) {
                case 1: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.operationId = value;
                        else
                            delete message.operationId;
                        continue;
                    }
                case 2: {
                        if (wireType !== 0)
                            break;
                        if (value = reader.int32())
                            message.status = value;
                        else
                            delete message.status;
                        continue;
                    }
                case 3: {
                        if (wireType !== 0)
                            break;
                        if (value = reader.int32())
                            message.errorCode = value;
                        else
                            delete message.errorCode;
                        continue;
                    }
                case 4: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.errorMessage = value;
                        else
                            delete message.errorMessage;
                        continue;
                    }
                case 6: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.farmVersion = value;
                        else
                            delete message.farmVersion;
                        continue;
                    }
                case 7: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.requestId = value;
                        else
                            delete message.requestId;
                        continue;
                    }
                case 15: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.bytes()).length)
                            message.payload = value;
                        else
                            delete message.payload;
                        continue;
                    }
                }
                reader.skipType(wireType, _depth, tag);
                if (!reader.discardUnknown) {
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
            }
            if (_end !== $undefined)
                throw $Error("missing end group");
            return message;
        };

        /**
         * Decodes a CommandResultEnvelope message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof common.CommandResultEnvelope
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {common.CommandResultEnvelope & common.CommandResultEnvelope.$Shape} CommandResultEnvelope
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        CommandResultEnvelope.decodeDelimited = function(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a CommandResultEnvelope message.
         * @function verify
         * @memberof common.CommandResultEnvelope
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        CommandResultEnvelope.verify = function (message, _depth) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                return "max depth exceeded";
            if (message.operationId != null && $Object.hasOwnProperty.call(message, "operationId"))
                if (!$util.isString(message.operationId))
                    return "operationId: string expected";
            if (message.status != null && $Object.hasOwnProperty.call(message, "status"))
                if (typeof message.status !== "number" || (message.status | 0) !== message.status)
                    return "status: enum value expected";
            if (message.errorCode != null && $Object.hasOwnProperty.call(message, "errorCode"))
                if (typeof message.errorCode !== "number" || (message.errorCode | 0) !== message.errorCode)
                    return "errorCode: enum value expected";
            if (message.errorMessage != null && $Object.hasOwnProperty.call(message, "errorMessage"))
                if (!$util.isString(message.errorMessage))
                    return "errorMessage: string expected";
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion"))
                if (!$util.isInteger(message.farmVersion) && !(message.farmVersion && $util.isInteger(message.farmVersion.low) && $util.isInteger(message.farmVersion.high)))
                    return "farmVersion: integer|Long expected";
            if (message.requestId != null && $Object.hasOwnProperty.call(message, "requestId"))
                if (!$util.isString(message.requestId))
                    return "requestId: string expected";
            if (message.payload != null && $Object.hasOwnProperty.call(message, "payload"))
                if (!(message.payload && typeof message.payload.length === "number" || $util.isString(message.payload)))
                    return "payload: buffer expected";
            return null;
        };

        /**
         * Creates a CommandResultEnvelope message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof common.CommandResultEnvelope
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {common.CommandResultEnvelope} CommandResultEnvelope
         */
        CommandResultEnvelope.fromObject = function (object, _depth) {
            if (object instanceof $root.common.CommandResultEnvelope)
                return object;
            if (!$util.isObject(object))
                throw $TypeError(".common.CommandResultEnvelope: object expected");
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var message = new $root.common.CommandResultEnvelope();
            if (object.operationId != null)
                if (typeof object.operationId !== "string" || object.operationId.length)
                    message.operationId = $String(object.operationId);
            if (object.status !== 0 && (typeof object.status !== "string" || $root.common.CommandStatus[object.status] !== 0))
                switch (object.status) {
                case "COMMAND_STATUS_UNSPECIFIED":
                case 0:
                    message.status = 0;
                    break;
                case "SUCCEEDED":
                case 1:
                    message.status = 1;
                    break;
                case "PROCESSING":
                case 2:
                    message.status = 2;
                    break;
                case "NO_CHANGE":
                case 3:
                    message.status = 3;
                    break;
                case "REJECTED":
                case 4:
                    message.status = 4;
                    break;
                case "UNAVAILABLE":
                case 5:
                    message.status = 5;
                    break;
                case "ACCEPTED":
                case 6:
                    message.status = 6;
                    break;
                default:
                    if (typeof object.status === "number" && (object.status | 0) === object.status)
                        message.status = object.status;
                }
            if (object.errorCode !== 0 && (typeof object.errorCode !== "string" || $root.common.ErrorCode[object.errorCode] !== 0))
                switch (object.errorCode) {
                case "ERROR_CODE_UNSPECIFIED":
                case 0:
                    message.errorCode = 0;
                    break;
                case "OK":
                case 1:
                    message.errorCode = 1;
                    break;
                case "ERR_BAD_FRAME":
                case 1001:
                    message.errorCode = 1001;
                    break;
                case "ERR_DECODE":
                case 1002:
                    message.errorCode = 1002;
                    break;
                case "ERR_UNSUPPORTED_VERSION":
                case 1003:
                    message.errorCode = 1003;
                    break;
                case "ERR_MESSAGE_TOO_LARGE":
                case 1004:
                    message.errorCode = 1004;
                    break;
                case "ERR_UNKNOWN_COMMAND":
                case 1005:
                    message.errorCode = 1005;
                    break;
                case "ERR_RATE_LIMITED":
                case 1006:
                    message.errorCode = 1006;
                    break;
                case "ERR_UNAUTHENTICATED":
                case 2001:
                    message.errorCode = 2001;
                    break;
                case "ERR_SESSION_EXPIRED":
                case 2002:
                    message.errorCode = 2002;
                    break;
                case "ERR_SESSION_PERMIT_INVALID":
                case 2003:
                    message.errorCode = 2003;
                    break;
                case "ERR_LOGIN_ELSEWHERE":
                case 2004:
                    message.errorCode = 2004;
                    break;
                case "ERR_CONNECTION_TICKET_INVALID":
                case 2005:
                    message.errorCode = 2005;
                    break;
                case "ERR_NOT_FRIEND":
                case 3001:
                    message.errorCode = 3001;
                    break;
                case "ERR_FRIEND_PERMIT_INVALID":
                case 3002:
                    message.errorCode = 3002;
                    break;
                case "ERR_FRIEND_LIMIT":
                case 3003:
                    message.errorCode = 3003;
                    break;
                case "ERR_SHARE_LINK_INVALID":
                case 3004:
                    message.errorCode = 3004;
                    break;
                case "ERR_PERMISSION_DENIED":
                case 3005:
                    message.errorCode = 3005;
                    break;
                case "ERR_INSUFFICIENT_COIN":
                case 4001:
                    message.errorCode = 4001;
                    break;
                case "ERR_INSUFFICIENT_ITEM":
                case 4002:
                    message.errorCode = 4002;
                    break;
                case "ERR_OFFER_NOT_FOUND":
                case 4003:
                    message.errorCode = 4003;
                    break;
                case "ERR_RULE_VERSION_MISMATCH":
                case 4004:
                    message.errorCode = 4004;
                    break;
                case "ERR_PLOT_NOT_EMPTY":
                case 5001:
                    message.errorCode = 5001;
                    break;
                case "ERR_PLOT_EMPTY":
                case 5002:
                    message.errorCode = 5002;
                    break;
                case "ERR_CROP_NOT_MATURE":
                case 5003:
                    message.errorCode = 5003;
                    break;
                case "ERR_VERSION_CONFLICT":
                case 5004:
                    message.errorCode = 5004;
                    break;
                case "ERR_PLOT_NOT_FOUND":
                case 5005:
                    message.errorCode = 5005;
                    break;
                case "ERR_FARM_FULL":
                case 6001:
                    message.errorCode = 6001;
                    break;
                case "ERR_NOT_IN_FARM":
                case 6002:
                    message.errorCode = 6002;
                    break;
                case "ERR_ACTOR_REDIRECT":
                case 6003:
                    message.errorCode = 6003;
                    break;
                case "ERR_DOWNLINK_OVERFLOW":
                case 6004:
                    message.errorCode = 6004;
                    break;
                case "ERR_STEAL_ALREADY_DONE":
                case 7001:
                    message.errorCode = 7001;
                    break;
                case "ERR_STEAL_QUOTA_EXHAUSTED":
                case 7002:
                    message.errorCode = 7002;
                    break;
                case "ERR_STEAL_BLOCKED_BY_PET":
                case 7003:
                    message.errorCode = 7003;
                    break;
                case "ERR_SETTLEMENT_NOT_FOUND":
                case 7004:
                    message.errorCode = 7004;
                    break;
                case "ERR_INTERNAL":
                case 9001:
                    message.errorCode = 9001;
                    break;
                case "ERR_STORE_CAS_CONFLICT":
                case 9002:
                    message.errorCode = 9002;
                    break;
                case "ERR_STORE_UNAVAILABLE":
                case 9003:
                    message.errorCode = 9003;
                    break;
                case "ERR_TIMEOUT":
                case 9004:
                    message.errorCode = 9004;
                    break;
                default:
                    if (typeof object.errorCode === "number" && (object.errorCode | 0) === object.errorCode)
                        message.errorCode = object.errorCode;
                }
            if (object.errorMessage != null)
                if (typeof object.errorMessage !== "string" || object.errorMessage.length)
                    message.errorMessage = $String(object.errorMessage);
            if (object.farmVersion != null)
                if (typeof object.farmVersion === "object" ? object.farmVersion.low || object.farmVersion.high : $Number(object.farmVersion) !== 0)
                    if ($util.Long)
                        message.farmVersion = $util.Long.fromValue(object.farmVersion, true);
                    else if (typeof object.farmVersion === "string")
                        message.farmVersion = $parseInt(object.farmVersion, 10);
                    else if (typeof object.farmVersion === "number")
                        message.farmVersion = object.farmVersion;
                    else if (typeof object.farmVersion === "object")
                        message.farmVersion = new $util.LongBits(object.farmVersion.low >>> 0, object.farmVersion.high >>> 0).toNumber(true);
            if (object.requestId != null)
                if (typeof object.requestId !== "string" || object.requestId.length)
                    message.requestId = $String(object.requestId);
            if (object.payload != null)
                if (object.payload.length)
                    if (typeof object.payload === "string")
                        $util.base64.decode(object.payload, message.payload = $util.newBuffer($util.base64.length(object.payload)), 0);
                    else if (object.payload.length >= 0)
                        message.payload = object.payload;
            return message;
        };

        /**
         * Creates a plain object from a CommandResultEnvelope message. Also converts values to other types if specified.
         * @function toObject
         * @memberof common.CommandResultEnvelope
         * @static
         * @param {common.CommandResultEnvelope} message CommandResultEnvelope
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        CommandResultEnvelope.toObject = function (message, options, _depth) {
            if (!options)
                options = {};
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var object = {};
            if (options.defaults) {
                object.operationId = "";
                object.status = options.enums === $String ? "COMMAND_STATUS_UNSPECIFIED" : 0;
                object.errorCode = options.enums === $String ? "ERROR_CODE_UNSPECIFIED" : 0;
                object.errorMessage = "";
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.farmVersion = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.farmVersion = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                object.requestId = "";
                if (options.bytes === $String)
                    object.payload = "";
                else {
                    object.payload = [];
                    if (options.bytes !== $Array)
                        object.payload = $util.newBuffer(object.payload);
                }
            }
            if (message.operationId != null && $Object.hasOwnProperty.call(message, "operationId"))
                object.operationId = message.operationId;
            if (message.status != null && $Object.hasOwnProperty.call(message, "status"))
                object.status = options.enums === $String ? $root.common.CommandStatus[message.status] === $undefined ? message.status : $root.common.CommandStatus[message.status] : message.status;
            if (message.errorCode != null && $Object.hasOwnProperty.call(message, "errorCode"))
                object.errorCode = options.enums === $String ? $root.common.ErrorCode[message.errorCode] === $undefined ? message.errorCode : $root.common.ErrorCode[message.errorCode] : message.errorCode;
            if (message.errorMessage != null && $Object.hasOwnProperty.call(message, "errorMessage"))
                object.errorMessage = message.errorMessage;
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.farmVersion = typeof message.farmVersion === "number" ? $BigInt(message.farmVersion) : $util.Long.fromBits(message.farmVersion.low >>> 0, message.farmVersion.high >>> 0, true).toBigInt();
                else if (typeof message.farmVersion === "number")
                    object.farmVersion = options.longs === $String ? $String(message.farmVersion) : message.farmVersion;
                else
                    object.farmVersion = options.longs === $String ? $util.Long.prototype.toString.call(message.farmVersion) : options.longs === $Number ? new $util.LongBits(message.farmVersion.low >>> 0, message.farmVersion.high >>> 0).toNumber(true) : message.farmVersion;
            if (message.requestId != null && $Object.hasOwnProperty.call(message, "requestId"))
                object.requestId = message.requestId;
            if (message.payload != null && $Object.hasOwnProperty.call(message, "payload"))
                object.payload = options.bytes === $String ? $util.base64.encode(message.payload, 0, message.payload.length) : options.bytes === $Array ? $Array.prototype.slice.call(message.payload) : message.payload;
            return object;
        };

        /**
         * Converts this CommandResultEnvelope to JSON.
         * @function toJSON
         * @memberof common.CommandResultEnvelope
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        CommandResultEnvelope.prototype.toJSON = function() {
            return CommandResultEnvelope.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the type url for CommandResultEnvelope
         * @function getTypeUrl
         * @memberof common.CommandResultEnvelope
         * @static
         * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
         * @returns {string} The type url
         */
        CommandResultEnvelope.getTypeUrl = function(prefix) {
            if (prefix === $undefined)
                prefix = "type.googleapis.com";
            return prefix + "/common.CommandResultEnvelope";
        };

        return CommandResultEnvelope;
    })();

    /**
     * PushType enum.
     * @name common.PushType
     * @enum {number}
     * @property {number} PUSH_TYPE_UNSPECIFIED=0 PUSH_TYPE_UNSPECIFIED value
     * @property {number} FARM_SNAPSHOT=1 FARM_SNAPSHOT value
     * @property {number} FARM_CHANGE=2 FARM_CHANGE value
     * @property {number} KICK=3 KICK value
     * @property {number} SYSTEM_NOTICE=4 SYSTEM_NOTICE value
     * @property {number} OPERATION_RESULT=5 OPERATION_RESULT value
     * @property {number} RESYNC_REQUIRED=6 RESYNC_REQUIRED value
     */
    common.PushType = (function() {
        var valuesById = $Object.create(null), values = $Object.create(valuesById);
        values[valuesById[0] = "PUSH_TYPE_UNSPECIFIED"] = 0;
        values[valuesById[1] = "FARM_SNAPSHOT"] = 1;
        values[valuesById[2] = "FARM_CHANGE"] = 2;
        values[valuesById[3] = "KICK"] = 3;
        values[valuesById[4] = "SYSTEM_NOTICE"] = 4;
        values[valuesById[5] = "OPERATION_RESULT"] = 5;
        values[valuesById[6] = "RESYNC_REQUIRED"] = 6;
        return values;
    })();

    common.ServerPushEnvelope = (function() {

        /**
         * Properties of a ServerPushEnvelope.
         * @typedef {Object} common.ServerPushEnvelope.$Properties
         * @property {string|null} [subscriptionId] ServerPushEnvelope subscriptionId
         * @property {number|Long|null} [actorActivationEpoch] ServerPushEnvelope actorActivationEpoch
         * @property {number|Long|null} [presenceEpoch] ServerPushEnvelope presenceEpoch
         * @property {number|Long|null} [farmVersion] ServerPushEnvelope farmVersion
         * @property {common.PushType|null} [pushType] ServerPushEnvelope pushType
         * @property {Uint8Array|null} [payload] ServerPushEnvelope payload
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */

        /**
         * Properties of a ServerPushEnvelope.
         * @memberof common
         * @interface IServerPushEnvelope
         * @augments common.ServerPushEnvelope.$Properties
         * @deprecated Use common.ServerPushEnvelope.$Properties instead.
         */

        /**
         * Shape of a ServerPushEnvelope.
         * @typedef {common.ServerPushEnvelope.$Properties} common.ServerPushEnvelope.$Shape
         */

        /**
         * Constructs a new ServerPushEnvelope.
         * @memberof common
         * @classdesc Represents a ServerPushEnvelope.
         * @constructor
         * @param {common.ServerPushEnvelope.$Properties=} [properties] Properties to set
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */
        var ServerPushEnvelope = function (properties) {
            if (properties)
                for (var keys = $Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null && keys[i] !== "__proto__")
                        this[keys[i]] = properties[keys[i]];
        };

        /**
         * ServerPushEnvelope subscriptionId.
         * @member {string} subscriptionId
         * @memberof common.ServerPushEnvelope
         * @instance
         */
        ServerPushEnvelope.prototype.subscriptionId = "";

        /**
         * ServerPushEnvelope actorActivationEpoch.
         * @member {number|Long} actorActivationEpoch
         * @memberof common.ServerPushEnvelope
         * @instance
         */
        ServerPushEnvelope.prototype.actorActivationEpoch = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * ServerPushEnvelope presenceEpoch.
         * @member {number|Long} presenceEpoch
         * @memberof common.ServerPushEnvelope
         * @instance
         */
        ServerPushEnvelope.prototype.presenceEpoch = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * ServerPushEnvelope farmVersion.
         * @member {number|Long} farmVersion
         * @memberof common.ServerPushEnvelope
         * @instance
         */
        ServerPushEnvelope.prototype.farmVersion = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * ServerPushEnvelope pushType.
         * @member {common.PushType} pushType
         * @memberof common.ServerPushEnvelope
         * @instance
         */
        ServerPushEnvelope.prototype.pushType = 0;

        /**
         * ServerPushEnvelope payload.
         * @member {Uint8Array} payload
         * @memberof common.ServerPushEnvelope
         * @instance
         */
        ServerPushEnvelope.prototype.payload = $util.newBuffer([]);

        /**
         * Creates a new ServerPushEnvelope instance using the specified properties.
         * @function create
         * @memberof common.ServerPushEnvelope
         * @static
         * @param {common.ServerPushEnvelope.$Properties=} [properties] Properties to set
         * @returns {common.ServerPushEnvelope} ServerPushEnvelope instance
         * @type {{
         *   (properties: common.ServerPushEnvelope.$Shape): common.ServerPushEnvelope & common.ServerPushEnvelope.$Shape;
         *   (properties?: common.ServerPushEnvelope.$Properties): common.ServerPushEnvelope;
         * }}
         */
        ServerPushEnvelope.create = function(properties) {
            return new ServerPushEnvelope(properties);
        };

        /**
         * Encodes the specified ServerPushEnvelope message. Does not implicitly {@link common.ServerPushEnvelope.verify|verify} messages.
         * @function encode
         * @memberof common.ServerPushEnvelope
         * @static
         * @param {common.ServerPushEnvelope.$Properties} message ServerPushEnvelope message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ServerPushEnvelope.encode = function (message, writer, _depth) {
            if (!writer)
                writer = $Writer.create();
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            if (message.subscriptionId != null && $Object.hasOwnProperty.call(message, "subscriptionId") && message.subscriptionId !== "")
                writer.uint32(/* id 2, wireType 2 =*/18).string(message.subscriptionId);
            if (message.actorActivationEpoch != null && $Object.hasOwnProperty.call(message, "actorActivationEpoch") && (typeof message.actorActivationEpoch === "object" ? message.actorActivationEpoch.low || message.actorActivationEpoch.high : message.actorActivationEpoch !== 0))
                writer.uint32(/* id 3, wireType 0 =*/24).uint64(message.actorActivationEpoch);
            if (message.presenceEpoch != null && $Object.hasOwnProperty.call(message, "presenceEpoch") && (typeof message.presenceEpoch === "object" ? message.presenceEpoch.low || message.presenceEpoch.high : message.presenceEpoch !== 0))
                writer.uint32(/* id 4, wireType 0 =*/32).uint64(message.presenceEpoch);
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion") && (typeof message.farmVersion === "object" ? message.farmVersion.low || message.farmVersion.high : message.farmVersion !== 0))
                writer.uint32(/* id 5, wireType 0 =*/40).uint64(message.farmVersion);
            if (message.pushType != null && $Object.hasOwnProperty.call(message, "pushType") && message.pushType !== 0)
                writer.uint32(/* id 6, wireType 0 =*/48).int32(message.pushType);
            if (message.payload != null && $Object.hasOwnProperty.call(message, "payload") && message.payload.length)
                writer.uint32(/* id 15, wireType 2 =*/122).bytes(message.payload);
            if (message.$unknowns != null && $Object.hasOwnProperty.call(message, "$unknowns"))
                for (var i = 0; i < message.$unknowns.length; ++i)
                    writer.raw(message.$unknowns[i]);
            return writer;
        };

        /**
         * Encodes the specified ServerPushEnvelope message, length delimited. Does not implicitly {@link common.ServerPushEnvelope.verify|verify} messages.
         * @function encodeDelimited
         * @memberof common.ServerPushEnvelope
         * @static
         * @param {common.ServerPushEnvelope.$Properties} message ServerPushEnvelope message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ServerPushEnvelope.encodeDelimited = function(message, writer) {
            return this.encode(message, (writer || $Writer.create()).fork()).ldelim();
        };

        /**
         * Decodes a ServerPushEnvelope message from the specified reader or buffer.
         * @function decode
         * @memberof common.ServerPushEnvelope
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {common.ServerPushEnvelope & common.ServerPushEnvelope.$Shape} ServerPushEnvelope
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ServerPushEnvelope.decode = function (reader, length, _end, _depth, _target) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $Reader.recursionLimit)
                throw $Error("max depth exceeded");
            var end = length === $undefined ? reader.len : reader.pos + length, message = _target || new $root.common.ServerPushEnvelope(), value;
            while (reader.pos < end) {
                var start = reader.pos;
                var tag = reader.tag();
                if (tag === _end) {
                    _end = $undefined;
                    break;
                }
                var wireType = tag & 7;
                switch (tag >>>= 3) {
                case 2: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.subscriptionId = value;
                        else
                            delete message.subscriptionId;
                        continue;
                    }
                case 3: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.actorActivationEpoch = value;
                        else
                            delete message.actorActivationEpoch;
                        continue;
                    }
                case 4: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.presenceEpoch = value;
                        else
                            delete message.presenceEpoch;
                        continue;
                    }
                case 5: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.farmVersion = value;
                        else
                            delete message.farmVersion;
                        continue;
                    }
                case 6: {
                        if (wireType !== 0)
                            break;
                        if (value = reader.int32())
                            message.pushType = value;
                        else
                            delete message.pushType;
                        continue;
                    }
                case 15: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.bytes()).length)
                            message.payload = value;
                        else
                            delete message.payload;
                        continue;
                    }
                }
                reader.skipType(wireType, _depth, tag);
                if (!reader.discardUnknown) {
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
            }
            if (_end !== $undefined)
                throw $Error("missing end group");
            return message;
        };

        /**
         * Decodes a ServerPushEnvelope message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof common.ServerPushEnvelope
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {common.ServerPushEnvelope & common.ServerPushEnvelope.$Shape} ServerPushEnvelope
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ServerPushEnvelope.decodeDelimited = function(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a ServerPushEnvelope message.
         * @function verify
         * @memberof common.ServerPushEnvelope
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        ServerPushEnvelope.verify = function (message, _depth) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                return "max depth exceeded";
            if (message.subscriptionId != null && $Object.hasOwnProperty.call(message, "subscriptionId"))
                if (!$util.isString(message.subscriptionId))
                    return "subscriptionId: string expected";
            if (message.actorActivationEpoch != null && $Object.hasOwnProperty.call(message, "actorActivationEpoch"))
                if (!$util.isInteger(message.actorActivationEpoch) && !(message.actorActivationEpoch && $util.isInteger(message.actorActivationEpoch.low) && $util.isInteger(message.actorActivationEpoch.high)))
                    return "actorActivationEpoch: integer|Long expected";
            if (message.presenceEpoch != null && $Object.hasOwnProperty.call(message, "presenceEpoch"))
                if (!$util.isInteger(message.presenceEpoch) && !(message.presenceEpoch && $util.isInteger(message.presenceEpoch.low) && $util.isInteger(message.presenceEpoch.high)))
                    return "presenceEpoch: integer|Long expected";
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion"))
                if (!$util.isInteger(message.farmVersion) && !(message.farmVersion && $util.isInteger(message.farmVersion.low) && $util.isInteger(message.farmVersion.high)))
                    return "farmVersion: integer|Long expected";
            if (message.pushType != null && $Object.hasOwnProperty.call(message, "pushType"))
                if (typeof message.pushType !== "number" || (message.pushType | 0) !== message.pushType)
                    return "pushType: enum value expected";
            if (message.payload != null && $Object.hasOwnProperty.call(message, "payload"))
                if (!(message.payload && typeof message.payload.length === "number" || $util.isString(message.payload)))
                    return "payload: buffer expected";
            return null;
        };

        /**
         * Creates a ServerPushEnvelope message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof common.ServerPushEnvelope
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {common.ServerPushEnvelope} ServerPushEnvelope
         */
        ServerPushEnvelope.fromObject = function (object, _depth) {
            if (object instanceof $root.common.ServerPushEnvelope)
                return object;
            if (!$util.isObject(object))
                throw $TypeError(".common.ServerPushEnvelope: object expected");
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var message = new $root.common.ServerPushEnvelope();
            if (object.subscriptionId != null)
                if (typeof object.subscriptionId !== "string" || object.subscriptionId.length)
                    message.subscriptionId = $String(object.subscriptionId);
            if (object.actorActivationEpoch != null)
                if (typeof object.actorActivationEpoch === "object" ? object.actorActivationEpoch.low || object.actorActivationEpoch.high : $Number(object.actorActivationEpoch) !== 0)
                    if ($util.Long)
                        message.actorActivationEpoch = $util.Long.fromValue(object.actorActivationEpoch, true);
                    else if (typeof object.actorActivationEpoch === "string")
                        message.actorActivationEpoch = $parseInt(object.actorActivationEpoch, 10);
                    else if (typeof object.actorActivationEpoch === "number")
                        message.actorActivationEpoch = object.actorActivationEpoch;
                    else if (typeof object.actorActivationEpoch === "object")
                        message.actorActivationEpoch = new $util.LongBits(object.actorActivationEpoch.low >>> 0, object.actorActivationEpoch.high >>> 0).toNumber(true);
            if (object.presenceEpoch != null)
                if (typeof object.presenceEpoch === "object" ? object.presenceEpoch.low || object.presenceEpoch.high : $Number(object.presenceEpoch) !== 0)
                    if ($util.Long)
                        message.presenceEpoch = $util.Long.fromValue(object.presenceEpoch, true);
                    else if (typeof object.presenceEpoch === "string")
                        message.presenceEpoch = $parseInt(object.presenceEpoch, 10);
                    else if (typeof object.presenceEpoch === "number")
                        message.presenceEpoch = object.presenceEpoch;
                    else if (typeof object.presenceEpoch === "object")
                        message.presenceEpoch = new $util.LongBits(object.presenceEpoch.low >>> 0, object.presenceEpoch.high >>> 0).toNumber(true);
            if (object.farmVersion != null)
                if (typeof object.farmVersion === "object" ? object.farmVersion.low || object.farmVersion.high : $Number(object.farmVersion) !== 0)
                    if ($util.Long)
                        message.farmVersion = $util.Long.fromValue(object.farmVersion, true);
                    else if (typeof object.farmVersion === "string")
                        message.farmVersion = $parseInt(object.farmVersion, 10);
                    else if (typeof object.farmVersion === "number")
                        message.farmVersion = object.farmVersion;
                    else if (typeof object.farmVersion === "object")
                        message.farmVersion = new $util.LongBits(object.farmVersion.low >>> 0, object.farmVersion.high >>> 0).toNumber(true);
            if (object.pushType !== 0 && (typeof object.pushType !== "string" || $root.common.PushType[object.pushType] !== 0))
                switch (object.pushType) {
                case "PUSH_TYPE_UNSPECIFIED":
                case 0:
                    message.pushType = 0;
                    break;
                case "FARM_SNAPSHOT":
                case 1:
                    message.pushType = 1;
                    break;
                case "FARM_CHANGE":
                case 2:
                    message.pushType = 2;
                    break;
                case "KICK":
                case 3:
                    message.pushType = 3;
                    break;
                case "SYSTEM_NOTICE":
                case 4:
                    message.pushType = 4;
                    break;
                case "OPERATION_RESULT":
                case 5:
                    message.pushType = 5;
                    break;
                case "RESYNC_REQUIRED":
                case 6:
                    message.pushType = 6;
                    break;
                default:
                    if (typeof object.pushType === "number" && (object.pushType | 0) === object.pushType)
                        message.pushType = object.pushType;
                }
            if (object.payload != null)
                if (object.payload.length)
                    if (typeof object.payload === "string")
                        $util.base64.decode(object.payload, message.payload = $util.newBuffer($util.base64.length(object.payload)), 0);
                    else if (object.payload.length >= 0)
                        message.payload = object.payload;
            return message;
        };

        /**
         * Creates a plain object from a ServerPushEnvelope message. Also converts values to other types if specified.
         * @function toObject
         * @memberof common.ServerPushEnvelope
         * @static
         * @param {common.ServerPushEnvelope} message ServerPushEnvelope
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        ServerPushEnvelope.toObject = function (message, options, _depth) {
            if (!options)
                options = {};
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var object = {};
            if (options.defaults) {
                object.subscriptionId = "";
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.actorActivationEpoch = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.actorActivationEpoch = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.presenceEpoch = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.presenceEpoch = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.farmVersion = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.farmVersion = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                object.pushType = options.enums === $String ? "PUSH_TYPE_UNSPECIFIED" : 0;
                if (options.bytes === $String)
                    object.payload = "";
                else {
                    object.payload = [];
                    if (options.bytes !== $Array)
                        object.payload = $util.newBuffer(object.payload);
                }
            }
            if (message.subscriptionId != null && $Object.hasOwnProperty.call(message, "subscriptionId"))
                object.subscriptionId = message.subscriptionId;
            if (message.actorActivationEpoch != null && $Object.hasOwnProperty.call(message, "actorActivationEpoch"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.actorActivationEpoch = typeof message.actorActivationEpoch === "number" ? $BigInt(message.actorActivationEpoch) : $util.Long.fromBits(message.actorActivationEpoch.low >>> 0, message.actorActivationEpoch.high >>> 0, true).toBigInt();
                else if (typeof message.actorActivationEpoch === "number")
                    object.actorActivationEpoch = options.longs === $String ? $String(message.actorActivationEpoch) : message.actorActivationEpoch;
                else
                    object.actorActivationEpoch = options.longs === $String ? $util.Long.prototype.toString.call(message.actorActivationEpoch) : options.longs === $Number ? new $util.LongBits(message.actorActivationEpoch.low >>> 0, message.actorActivationEpoch.high >>> 0).toNumber(true) : message.actorActivationEpoch;
            if (message.presenceEpoch != null && $Object.hasOwnProperty.call(message, "presenceEpoch"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.presenceEpoch = typeof message.presenceEpoch === "number" ? $BigInt(message.presenceEpoch) : $util.Long.fromBits(message.presenceEpoch.low >>> 0, message.presenceEpoch.high >>> 0, true).toBigInt();
                else if (typeof message.presenceEpoch === "number")
                    object.presenceEpoch = options.longs === $String ? $String(message.presenceEpoch) : message.presenceEpoch;
                else
                    object.presenceEpoch = options.longs === $String ? $util.Long.prototype.toString.call(message.presenceEpoch) : options.longs === $Number ? new $util.LongBits(message.presenceEpoch.low >>> 0, message.presenceEpoch.high >>> 0).toNumber(true) : message.presenceEpoch;
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.farmVersion = typeof message.farmVersion === "number" ? $BigInt(message.farmVersion) : $util.Long.fromBits(message.farmVersion.low >>> 0, message.farmVersion.high >>> 0, true).toBigInt();
                else if (typeof message.farmVersion === "number")
                    object.farmVersion = options.longs === $String ? $String(message.farmVersion) : message.farmVersion;
                else
                    object.farmVersion = options.longs === $String ? $util.Long.prototype.toString.call(message.farmVersion) : options.longs === $Number ? new $util.LongBits(message.farmVersion.low >>> 0, message.farmVersion.high >>> 0).toNumber(true) : message.farmVersion;
            if (message.pushType != null && $Object.hasOwnProperty.call(message, "pushType"))
                object.pushType = options.enums === $String ? $root.common.PushType[message.pushType] === $undefined ? message.pushType : $root.common.PushType[message.pushType] : message.pushType;
            if (message.payload != null && $Object.hasOwnProperty.call(message, "payload"))
                object.payload = options.bytes === $String ? $util.base64.encode(message.payload, 0, message.payload.length) : options.bytes === $Array ? $Array.prototype.slice.call(message.payload) : message.payload;
            return object;
        };

        /**
         * Converts this ServerPushEnvelope to JSON.
         * @function toJSON
         * @memberof common.ServerPushEnvelope
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        ServerPushEnvelope.prototype.toJSON = function() {
            return ServerPushEnvelope.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the type url for ServerPushEnvelope
         * @function getTypeUrl
         * @memberof common.ServerPushEnvelope
         * @static
         * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
         * @returns {string} The type url
         */
        ServerPushEnvelope.getTypeUrl = function(prefix) {
            if (prefix === $undefined)
                prefix = "type.googleapis.com";
            return prefix + "/common.ServerPushEnvelope";
        };

        return ServerPushEnvelope;
    })();

    return common;
})();

$root.farm = (function() {

    /**
     * Namespace farm.
     * @exports farm
     * @namespace
     */
    var farm = {};

    farm.PlotView = (function() {

        /**
         * Properties of a PlotView.
         * @typedef {Object} farm.PlotView.$Properties
         * @property {number|null} [plotIndex] PlotView plotIndex
         * @property {number|Long|null} [plotVersion] PlotView plotVersion
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */

        /**
         * Properties of a PlotView.
         * @memberof farm
         * @interface IPlotView
         * @augments farm.PlotView.$Properties
         * @deprecated Use farm.PlotView.$Properties instead.
         */

        /**
         * Shape of a PlotView.
         * @typedef {farm.PlotView.$Properties} farm.PlotView.$Shape
         */

        /**
         * Constructs a new PlotView.
         * @memberof farm
         * @classdesc Represents a PlotView.
         * @constructor
         * @param {farm.PlotView.$Properties=} [properties] Properties to set
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */
        var PlotView = function (properties) {
            if (properties)
                for (var keys = $Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null && keys[i] !== "__proto__")
                        this[keys[i]] = properties[keys[i]];
        };

        /**
         * PlotView plotIndex.
         * @member {number} plotIndex
         * @memberof farm.PlotView
         * @instance
         */
        PlotView.prototype.plotIndex = 0;

        /**
         * PlotView plotVersion.
         * @member {number|Long} plotVersion
         * @memberof farm.PlotView
         * @instance
         */
        PlotView.prototype.plotVersion = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * Creates a new PlotView instance using the specified properties.
         * @function create
         * @memberof farm.PlotView
         * @static
         * @param {farm.PlotView.$Properties=} [properties] Properties to set
         * @returns {farm.PlotView} PlotView instance
         * @type {{
         *   (properties: farm.PlotView.$Shape): farm.PlotView & farm.PlotView.$Shape;
         *   (properties?: farm.PlotView.$Properties): farm.PlotView;
         * }}
         */
        PlotView.create = function(properties) {
            return new PlotView(properties);
        };

        /**
         * Encodes the specified PlotView message. Does not implicitly {@link farm.PlotView.verify|verify} messages.
         * @function encode
         * @memberof farm.PlotView
         * @static
         * @param {farm.PlotView.$Properties} message PlotView message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        PlotView.encode = function (message, writer, _depth) {
            if (!writer)
                writer = $Writer.create();
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            if (message.plotIndex != null && $Object.hasOwnProperty.call(message, "plotIndex") && message.plotIndex !== 0)
                writer.uint32(/* id 1, wireType 0 =*/8).uint32(message.plotIndex);
            if (message.plotVersion != null && $Object.hasOwnProperty.call(message, "plotVersion") && (typeof message.plotVersion === "object" ? message.plotVersion.low || message.plotVersion.high : message.plotVersion !== 0))
                writer.uint32(/* id 2, wireType 0 =*/16).uint64(message.plotVersion);
            if (message.$unknowns != null && $Object.hasOwnProperty.call(message, "$unknowns"))
                for (var i = 0; i < message.$unknowns.length; ++i)
                    writer.raw(message.$unknowns[i]);
            return writer;
        };

        /**
         * Encodes the specified PlotView message, length delimited. Does not implicitly {@link farm.PlotView.verify|verify} messages.
         * @function encodeDelimited
         * @memberof farm.PlotView
         * @static
         * @param {farm.PlotView.$Properties} message PlotView message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        PlotView.encodeDelimited = function(message, writer) {
            return this.encode(message, (writer || $Writer.create()).fork()).ldelim();
        };

        /**
         * Decodes a PlotView message from the specified reader or buffer.
         * @function decode
         * @memberof farm.PlotView
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {farm.PlotView & farm.PlotView.$Shape} PlotView
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        PlotView.decode = function (reader, length, _end, _depth, _target) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $Reader.recursionLimit)
                throw $Error("max depth exceeded");
            var end = length === $undefined ? reader.len : reader.pos + length, message = _target || new $root.farm.PlotView(), value;
            while (reader.pos < end) {
                var start = reader.pos;
                var tag = reader.tag();
                if (tag === _end) {
                    _end = $undefined;
                    break;
                }
                var wireType = tag & 7;
                switch (tag >>>= 3) {
                case 1: {
                        if (wireType !== 0)
                            break;
                        if (value = reader.uint32())
                            message.plotIndex = value;
                        else
                            delete message.plotIndex;
                        continue;
                    }
                case 2: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.plotVersion = value;
                        else
                            delete message.plotVersion;
                        continue;
                    }
                }
                reader.skipType(wireType, _depth, tag);
                if (!reader.discardUnknown) {
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
            }
            if (_end !== $undefined)
                throw $Error("missing end group");
            return message;
        };

        /**
         * Decodes a PlotView message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof farm.PlotView
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {farm.PlotView & farm.PlotView.$Shape} PlotView
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        PlotView.decodeDelimited = function(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a PlotView message.
         * @function verify
         * @memberof farm.PlotView
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        PlotView.verify = function (message, _depth) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                return "max depth exceeded";
            if (message.plotIndex != null && $Object.hasOwnProperty.call(message, "plotIndex"))
                if (!$util.isInteger(message.plotIndex))
                    return "plotIndex: integer expected";
            if (message.plotVersion != null && $Object.hasOwnProperty.call(message, "plotVersion"))
                if (!$util.isInteger(message.plotVersion) && !(message.plotVersion && $util.isInteger(message.plotVersion.low) && $util.isInteger(message.plotVersion.high)))
                    return "plotVersion: integer|Long expected";
            return null;
        };

        /**
         * Creates a PlotView message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof farm.PlotView
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {farm.PlotView} PlotView
         */
        PlotView.fromObject = function (object, _depth) {
            if (object instanceof $root.farm.PlotView)
                return object;
            if (!$util.isObject(object))
                throw $TypeError(".farm.PlotView: object expected");
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var message = new $root.farm.PlotView();
            if (object.plotIndex != null)
                if ($Number(object.plotIndex) !== 0)
                    message.plotIndex = object.plotIndex >>> 0;
            if (object.plotVersion != null)
                if (typeof object.plotVersion === "object" ? object.plotVersion.low || object.plotVersion.high : $Number(object.plotVersion) !== 0)
                    if ($util.Long)
                        message.plotVersion = $util.Long.fromValue(object.plotVersion, true);
                    else if (typeof object.plotVersion === "string")
                        message.plotVersion = $parseInt(object.plotVersion, 10);
                    else if (typeof object.plotVersion === "number")
                        message.plotVersion = object.plotVersion;
                    else if (typeof object.plotVersion === "object")
                        message.plotVersion = new $util.LongBits(object.plotVersion.low >>> 0, object.plotVersion.high >>> 0).toNumber(true);
            return message;
        };

        /**
         * Creates a plain object from a PlotView message. Also converts values to other types if specified.
         * @function toObject
         * @memberof farm.PlotView
         * @static
         * @param {farm.PlotView} message PlotView
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        PlotView.toObject = function (message, options, _depth) {
            if (!options)
                options = {};
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var object = {};
            if (options.defaults) {
                object.plotIndex = 0;
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.plotVersion = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.plotVersion = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
            }
            if (message.plotIndex != null && $Object.hasOwnProperty.call(message, "plotIndex"))
                object.plotIndex = message.plotIndex;
            if (message.plotVersion != null && $Object.hasOwnProperty.call(message, "plotVersion"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.plotVersion = typeof message.plotVersion === "number" ? $BigInt(message.plotVersion) : $util.Long.fromBits(message.plotVersion.low >>> 0, message.plotVersion.high >>> 0, true).toBigInt();
                else if (typeof message.plotVersion === "number")
                    object.plotVersion = options.longs === $String ? $String(message.plotVersion) : message.plotVersion;
                else
                    object.plotVersion = options.longs === $String ? $util.Long.prototype.toString.call(message.plotVersion) : options.longs === $Number ? new $util.LongBits(message.plotVersion.low >>> 0, message.plotVersion.high >>> 0).toNumber(true) : message.plotVersion;
            return object;
        };

        /**
         * Converts this PlotView to JSON.
         * @function toJSON
         * @memberof farm.PlotView
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        PlotView.prototype.toJSON = function() {
            return PlotView.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the type url for PlotView
         * @function getTypeUrl
         * @memberof farm.PlotView
         * @static
         * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
         * @returns {string} The type url
         */
        PlotView.getTypeUrl = function(prefix) {
            if (prefix === $undefined)
                prefix = "type.googleapis.com";
            return prefix + "/farm.PlotView";
        };

        return PlotView;
    })();

    farm.FarmSnapshot = (function() {

        /**
         * Properties of a FarmSnapshot.
         * @typedef {Object} farm.FarmSnapshot.$Properties
         * @property {number|Long|null} [ownerId] FarmSnapshot ownerId
         * @property {number|Long|null} [snapshotTimeMs] FarmSnapshot snapshotTimeMs
         * @property {Array.<farm.PlotView.$Properties>|null} [plots] FarmSnapshot plots
         * @property {number|Long|null} [farmVersion] FarmSnapshot farmVersion
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */

        /**
         * Properties of a FarmSnapshot.
         * @memberof farm
         * @interface IFarmSnapshot
         * @augments farm.FarmSnapshot.$Properties
         * @deprecated Use farm.FarmSnapshot.$Properties instead.
         */

        /**
         * Shape of a FarmSnapshot.
         * @typedef {farm.FarmSnapshot.$Properties} farm.FarmSnapshot.$Shape
         */

        /**
         * Constructs a new FarmSnapshot.
         * @memberof farm
         * @classdesc Represents a FarmSnapshot.
         * @constructor
         * @param {farm.FarmSnapshot.$Properties=} [properties] Properties to set
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */
        var FarmSnapshot = function (properties) {
            this.plots = [];
            if (properties)
                for (var keys = $Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null && keys[i] !== "__proto__")
                        this[keys[i]] = properties[keys[i]];
        };

        /**
         * FarmSnapshot ownerId.
         * @member {number|Long} ownerId
         * @memberof farm.FarmSnapshot
         * @instance
         */
        FarmSnapshot.prototype.ownerId = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * FarmSnapshot snapshotTimeMs.
         * @member {number|Long} snapshotTimeMs
         * @memberof farm.FarmSnapshot
         * @instance
         */
        FarmSnapshot.prototype.snapshotTimeMs = $util.Long ? $util.Long.fromBits(0,0,false) : 0;

        /**
         * FarmSnapshot plots.
         * @member {Array.<farm.PlotView.$Properties>} plots
         * @memberof farm.FarmSnapshot
         * @instance
         */
        FarmSnapshot.prototype.plots = $util.emptyArray;

        /**
         * FarmSnapshot farmVersion.
         * @member {number|Long} farmVersion
         * @memberof farm.FarmSnapshot
         * @instance
         */
        FarmSnapshot.prototype.farmVersion = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * Creates a new FarmSnapshot instance using the specified properties.
         * @function create
         * @memberof farm.FarmSnapshot
         * @static
         * @param {farm.FarmSnapshot.$Properties=} [properties] Properties to set
         * @returns {farm.FarmSnapshot} FarmSnapshot instance
         * @type {{
         *   (properties: farm.FarmSnapshot.$Shape): farm.FarmSnapshot & farm.FarmSnapshot.$Shape;
         *   (properties?: farm.FarmSnapshot.$Properties): farm.FarmSnapshot;
         * }}
         */
        FarmSnapshot.create = function(properties) {
            return new FarmSnapshot(properties);
        };

        /**
         * Encodes the specified FarmSnapshot message. Does not implicitly {@link farm.FarmSnapshot.verify|verify} messages.
         * @function encode
         * @memberof farm.FarmSnapshot
         * @static
         * @param {farm.FarmSnapshot.$Properties} message FarmSnapshot message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        FarmSnapshot.encode = function (message, writer, _depth) {
            if (!writer)
                writer = $Writer.create();
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            if (message.ownerId != null && $Object.hasOwnProperty.call(message, "ownerId") && (typeof message.ownerId === "object" ? message.ownerId.low || message.ownerId.high : message.ownerId !== 0))
                writer.uint32(/* id 1, wireType 0 =*/8).uint64(message.ownerId);
            if (message.snapshotTimeMs != null && $Object.hasOwnProperty.call(message, "snapshotTimeMs") && (typeof message.snapshotTimeMs === "object" ? message.snapshotTimeMs.low || message.snapshotTimeMs.high : message.snapshotTimeMs !== 0))
                writer.uint32(/* id 2, wireType 0 =*/16).int64(message.snapshotTimeMs);
            if (message.plots != null && message.plots.length)
                for (var i = 0; i < message.plots.length; ++i)
                    $root.farm.PlotView.encode(message.plots[i], writer.uint32(/* id 3, wireType 2 =*/26).fork(), _depth + 1).ldelim();
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion") && (typeof message.farmVersion === "object" ? message.farmVersion.low || message.farmVersion.high : message.farmVersion !== 0))
                writer.uint32(/* id 4, wireType 0 =*/32).uint64(message.farmVersion);
            if (message.$unknowns != null && $Object.hasOwnProperty.call(message, "$unknowns"))
                for (var i = 0; i < message.$unknowns.length; ++i)
                    writer.raw(message.$unknowns[i]);
            return writer;
        };

        /**
         * Encodes the specified FarmSnapshot message, length delimited. Does not implicitly {@link farm.FarmSnapshot.verify|verify} messages.
         * @function encodeDelimited
         * @memberof farm.FarmSnapshot
         * @static
         * @param {farm.FarmSnapshot.$Properties} message FarmSnapshot message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        FarmSnapshot.encodeDelimited = function(message, writer) {
            return this.encode(message, (writer || $Writer.create()).fork()).ldelim();
        };

        /**
         * Decodes a FarmSnapshot message from the specified reader or buffer.
         * @function decode
         * @memberof farm.FarmSnapshot
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {farm.FarmSnapshot & farm.FarmSnapshot.$Shape} FarmSnapshot
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        FarmSnapshot.decode = function (reader, length, _end, _depth, _target) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $Reader.recursionLimit)
                throw $Error("max depth exceeded");
            var end = length === $undefined ? reader.len : reader.pos + length, message = _target || new $root.farm.FarmSnapshot(), value;
            while (reader.pos < end) {
                var start = reader.pos;
                var tag = reader.tag();
                if (tag === _end) {
                    _end = $undefined;
                    break;
                }
                var wireType = tag & 7;
                switch (tag >>>= 3) {
                case 1: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.ownerId = value;
                        else
                            delete message.ownerId;
                        continue;
                    }
                case 2: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.int64()) === "object" ? value.low || value.high : value !== 0)
                            message.snapshotTimeMs = value;
                        else
                            delete message.snapshotTimeMs;
                        continue;
                    }
                case 3: {
                        if (wireType !== 2)
                            break;
                        if (!(message.plots && message.plots.length))
                            message.plots = [];
                        message.plots.push($root.farm.PlotView.decode(reader, reader.uint32(), $undefined, _depth + 1));
                        continue;
                    }
                case 4: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.farmVersion = value;
                        else
                            delete message.farmVersion;
                        continue;
                    }
                }
                reader.skipType(wireType, _depth, tag);
                if (!reader.discardUnknown) {
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
            }
            if (_end !== $undefined)
                throw $Error("missing end group");
            return message;
        };

        /**
         * Decodes a FarmSnapshot message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof farm.FarmSnapshot
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {farm.FarmSnapshot & farm.FarmSnapshot.$Shape} FarmSnapshot
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        FarmSnapshot.decodeDelimited = function(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a FarmSnapshot message.
         * @function verify
         * @memberof farm.FarmSnapshot
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        FarmSnapshot.verify = function (message, _depth) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                return "max depth exceeded";
            if (message.ownerId != null && $Object.hasOwnProperty.call(message, "ownerId"))
                if (!$util.isInteger(message.ownerId) && !(message.ownerId && $util.isInteger(message.ownerId.low) && $util.isInteger(message.ownerId.high)))
                    return "ownerId: integer|Long expected";
            if (message.snapshotTimeMs != null && $Object.hasOwnProperty.call(message, "snapshotTimeMs"))
                if (!$util.isInteger(message.snapshotTimeMs) && !(message.snapshotTimeMs && $util.isInteger(message.snapshotTimeMs.low) && $util.isInteger(message.snapshotTimeMs.high)))
                    return "snapshotTimeMs: integer|Long expected";
            if (message.plots != null && $Object.hasOwnProperty.call(message, "plots")) {
                if (!$Array.isArray(message.plots))
                    return "plots: array expected";
                for (var i = 0; i < message.plots.length; ++i) {
                    var error = $root.farm.PlotView.verify(message.plots[i], _depth + 1);
                    if (error)
                        return "plots." + error;
                }
            }
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion"))
                if (!$util.isInteger(message.farmVersion) && !(message.farmVersion && $util.isInteger(message.farmVersion.low) && $util.isInteger(message.farmVersion.high)))
                    return "farmVersion: integer|Long expected";
            return null;
        };

        /**
         * Creates a FarmSnapshot message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof farm.FarmSnapshot
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {farm.FarmSnapshot} FarmSnapshot
         */
        FarmSnapshot.fromObject = function (object, _depth) {
            if (object instanceof $root.farm.FarmSnapshot)
                return object;
            if (!$util.isObject(object))
                throw $TypeError(".farm.FarmSnapshot: object expected");
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var message = new $root.farm.FarmSnapshot();
            if (object.ownerId != null)
                if (typeof object.ownerId === "object" ? object.ownerId.low || object.ownerId.high : $Number(object.ownerId) !== 0)
                    if ($util.Long)
                        message.ownerId = $util.Long.fromValue(object.ownerId, true);
                    else if (typeof object.ownerId === "string")
                        message.ownerId = $parseInt(object.ownerId, 10);
                    else if (typeof object.ownerId === "number")
                        message.ownerId = object.ownerId;
                    else if (typeof object.ownerId === "object")
                        message.ownerId = new $util.LongBits(object.ownerId.low >>> 0, object.ownerId.high >>> 0).toNumber(true);
            if (object.snapshotTimeMs != null)
                if (typeof object.snapshotTimeMs === "object" ? object.snapshotTimeMs.low || object.snapshotTimeMs.high : $Number(object.snapshotTimeMs) !== 0)
                    if ($util.Long)
                        message.snapshotTimeMs = $util.Long.fromValue(object.snapshotTimeMs, false);
                    else if (typeof object.snapshotTimeMs === "string")
                        message.snapshotTimeMs = $parseInt(object.snapshotTimeMs, 10);
                    else if (typeof object.snapshotTimeMs === "number")
                        message.snapshotTimeMs = object.snapshotTimeMs;
                    else if (typeof object.snapshotTimeMs === "object")
                        message.snapshotTimeMs = new $util.LongBits(object.snapshotTimeMs.low >>> 0, object.snapshotTimeMs.high >>> 0).toNumber();
            if (object.plots) {
                if (!$Array.isArray(object.plots))
                    throw $TypeError(".farm.FarmSnapshot.plots: array expected");
                message.plots = $Array(object.plots.length);
                for (var i = 0; i < object.plots.length; ++i) {
                    if (!$util.isObject(object.plots[i]))
                        throw $TypeError(".farm.FarmSnapshot.plots: object expected");
                    message.plots[i] = $root.farm.PlotView.fromObject(object.plots[i], _depth + 1);
                }
            }
            if (object.farmVersion != null)
                if (typeof object.farmVersion === "object" ? object.farmVersion.low || object.farmVersion.high : $Number(object.farmVersion) !== 0)
                    if ($util.Long)
                        message.farmVersion = $util.Long.fromValue(object.farmVersion, true);
                    else if (typeof object.farmVersion === "string")
                        message.farmVersion = $parseInt(object.farmVersion, 10);
                    else if (typeof object.farmVersion === "number")
                        message.farmVersion = object.farmVersion;
                    else if (typeof object.farmVersion === "object")
                        message.farmVersion = new $util.LongBits(object.farmVersion.low >>> 0, object.farmVersion.high >>> 0).toNumber(true);
            return message;
        };

        /**
         * Creates a plain object from a FarmSnapshot message. Also converts values to other types if specified.
         * @function toObject
         * @memberof farm.FarmSnapshot
         * @static
         * @param {farm.FarmSnapshot} message FarmSnapshot
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        FarmSnapshot.toObject = function (message, options, _depth) {
            if (!options)
                options = {};
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var object = {};
            if (options.arrays || options.defaults)
                object.plots = [];
            if (options.defaults) {
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.ownerId = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.ownerId = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                if ($util.Long) {
                    var long = new $util.Long(0, 0, false);
                    object.snapshotTimeMs = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.snapshotTimeMs = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.farmVersion = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.farmVersion = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
            }
            if (message.ownerId != null && $Object.hasOwnProperty.call(message, "ownerId"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.ownerId = typeof message.ownerId === "number" ? $BigInt(message.ownerId) : $util.Long.fromBits(message.ownerId.low >>> 0, message.ownerId.high >>> 0, true).toBigInt();
                else if (typeof message.ownerId === "number")
                    object.ownerId = options.longs === $String ? $String(message.ownerId) : message.ownerId;
                else
                    object.ownerId = options.longs === $String ? $util.Long.prototype.toString.call(message.ownerId) : options.longs === $Number ? new $util.LongBits(message.ownerId.low >>> 0, message.ownerId.high >>> 0).toNumber(true) : message.ownerId;
            if (message.snapshotTimeMs != null && $Object.hasOwnProperty.call(message, "snapshotTimeMs"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.snapshotTimeMs = typeof message.snapshotTimeMs === "number" ? $BigInt(message.snapshotTimeMs) : $util.Long.fromBits(message.snapshotTimeMs.low >>> 0, message.snapshotTimeMs.high >>> 0, false).toBigInt();
                else if (typeof message.snapshotTimeMs === "number")
                    object.snapshotTimeMs = options.longs === $String ? $String(message.snapshotTimeMs) : message.snapshotTimeMs;
                else
                    object.snapshotTimeMs = options.longs === $String ? $util.Long.prototype.toString.call(message.snapshotTimeMs) : options.longs === $Number ? new $util.LongBits(message.snapshotTimeMs.low >>> 0, message.snapshotTimeMs.high >>> 0).toNumber() : message.snapshotTimeMs;
            if (message.plots && message.plots.length) {
                object.plots = $Array(message.plots.length);
                for (var j = 0; j < message.plots.length; ++j)
                    object.plots[j] = $root.farm.PlotView.toObject(message.plots[j], options, _depth + 1);
            }
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.farmVersion = typeof message.farmVersion === "number" ? $BigInt(message.farmVersion) : $util.Long.fromBits(message.farmVersion.low >>> 0, message.farmVersion.high >>> 0, true).toBigInt();
                else if (typeof message.farmVersion === "number")
                    object.farmVersion = options.longs === $String ? $String(message.farmVersion) : message.farmVersion;
                else
                    object.farmVersion = options.longs === $String ? $util.Long.prototype.toString.call(message.farmVersion) : options.longs === $Number ? new $util.LongBits(message.farmVersion.low >>> 0, message.farmVersion.high >>> 0).toNumber(true) : message.farmVersion;
            return object;
        };

        /**
         * Converts this FarmSnapshot to JSON.
         * @function toJSON
         * @memberof farm.FarmSnapshot
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        FarmSnapshot.prototype.toJSON = function() {
            return FarmSnapshot.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the type url for FarmSnapshot
         * @function getTypeUrl
         * @memberof farm.FarmSnapshot
         * @static
         * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
         * @returns {string} The type url
         */
        FarmSnapshot.getTypeUrl = function(prefix) {
            if (prefix === $undefined)
                prefix = "type.googleapis.com";
            return prefix + "/farm.FarmSnapshot";
        };

        return FarmSnapshot;
    })();

    farm.FarmChange = (function() {

        /**
         * Properties of a FarmChange.
         * @typedef {Object} farm.FarmChange.$Properties
         * @property {number|Long|null} [ownerId] FarmChange ownerId
         * @property {number|Long|null} [farmVersion] FarmChange farmVersion
         * @property {Array.<farm.PlotView.$Properties>|null} [changedPlots] FarmChange changedPlots
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */

        /**
         * Properties of a FarmChange.
         * @memberof farm
         * @interface IFarmChange
         * @augments farm.FarmChange.$Properties
         * @deprecated Use farm.FarmChange.$Properties instead.
         */

        /**
         * Shape of a FarmChange.
         * @typedef {farm.FarmChange.$Properties} farm.FarmChange.$Shape
         */

        /**
         * Constructs a new FarmChange.
         * @memberof farm
         * @classdesc Represents a FarmChange.
         * @constructor
         * @param {farm.FarmChange.$Properties=} [properties] Properties to set
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */
        var FarmChange = function (properties) {
            this.changedPlots = [];
            if (properties)
                for (var keys = $Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null && keys[i] !== "__proto__")
                        this[keys[i]] = properties[keys[i]];
        };

        /**
         * FarmChange ownerId.
         * @member {number|Long} ownerId
         * @memberof farm.FarmChange
         * @instance
         */
        FarmChange.prototype.ownerId = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * FarmChange farmVersion.
         * @member {number|Long} farmVersion
         * @memberof farm.FarmChange
         * @instance
         */
        FarmChange.prototype.farmVersion = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * FarmChange changedPlots.
         * @member {Array.<farm.PlotView.$Properties>} changedPlots
         * @memberof farm.FarmChange
         * @instance
         */
        FarmChange.prototype.changedPlots = $util.emptyArray;

        /**
         * Creates a new FarmChange instance using the specified properties.
         * @function create
         * @memberof farm.FarmChange
         * @static
         * @param {farm.FarmChange.$Properties=} [properties] Properties to set
         * @returns {farm.FarmChange} FarmChange instance
         * @type {{
         *   (properties: farm.FarmChange.$Shape): farm.FarmChange & farm.FarmChange.$Shape;
         *   (properties?: farm.FarmChange.$Properties): farm.FarmChange;
         * }}
         */
        FarmChange.create = function(properties) {
            return new FarmChange(properties);
        };

        /**
         * Encodes the specified FarmChange message. Does not implicitly {@link farm.FarmChange.verify|verify} messages.
         * @function encode
         * @memberof farm.FarmChange
         * @static
         * @param {farm.FarmChange.$Properties} message FarmChange message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        FarmChange.encode = function (message, writer, _depth) {
            if (!writer)
                writer = $Writer.create();
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            if (message.ownerId != null && $Object.hasOwnProperty.call(message, "ownerId") && (typeof message.ownerId === "object" ? message.ownerId.low || message.ownerId.high : message.ownerId !== 0))
                writer.uint32(/* id 1, wireType 0 =*/8).uint64(message.ownerId);
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion") && (typeof message.farmVersion === "object" ? message.farmVersion.low || message.farmVersion.high : message.farmVersion !== 0))
                writer.uint32(/* id 2, wireType 0 =*/16).uint64(message.farmVersion);
            if (message.changedPlots != null && message.changedPlots.length)
                for (var i = 0; i < message.changedPlots.length; ++i)
                    $root.farm.PlotView.encode(message.changedPlots[i], writer.uint32(/* id 3, wireType 2 =*/26).fork(), _depth + 1).ldelim();
            if (message.$unknowns != null && $Object.hasOwnProperty.call(message, "$unknowns"))
                for (var i = 0; i < message.$unknowns.length; ++i)
                    writer.raw(message.$unknowns[i]);
            return writer;
        };

        /**
         * Encodes the specified FarmChange message, length delimited. Does not implicitly {@link farm.FarmChange.verify|verify} messages.
         * @function encodeDelimited
         * @memberof farm.FarmChange
         * @static
         * @param {farm.FarmChange.$Properties} message FarmChange message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        FarmChange.encodeDelimited = function(message, writer) {
            return this.encode(message, (writer || $Writer.create()).fork()).ldelim();
        };

        /**
         * Decodes a FarmChange message from the specified reader or buffer.
         * @function decode
         * @memberof farm.FarmChange
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {farm.FarmChange & farm.FarmChange.$Shape} FarmChange
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        FarmChange.decode = function (reader, length, _end, _depth, _target) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $Reader.recursionLimit)
                throw $Error("max depth exceeded");
            var end = length === $undefined ? reader.len : reader.pos + length, message = _target || new $root.farm.FarmChange(), value;
            while (reader.pos < end) {
                var start = reader.pos;
                var tag = reader.tag();
                if (tag === _end) {
                    _end = $undefined;
                    break;
                }
                var wireType = tag & 7;
                switch (tag >>>= 3) {
                case 1: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.ownerId = value;
                        else
                            delete message.ownerId;
                        continue;
                    }
                case 2: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.farmVersion = value;
                        else
                            delete message.farmVersion;
                        continue;
                    }
                case 3: {
                        if (wireType !== 2)
                            break;
                        if (!(message.changedPlots && message.changedPlots.length))
                            message.changedPlots = [];
                        message.changedPlots.push($root.farm.PlotView.decode(reader, reader.uint32(), $undefined, _depth + 1));
                        continue;
                    }
                }
                reader.skipType(wireType, _depth, tag);
                if (!reader.discardUnknown) {
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
            }
            if (_end !== $undefined)
                throw $Error("missing end group");
            return message;
        };

        /**
         * Decodes a FarmChange message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof farm.FarmChange
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {farm.FarmChange & farm.FarmChange.$Shape} FarmChange
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        FarmChange.decodeDelimited = function(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a FarmChange message.
         * @function verify
         * @memberof farm.FarmChange
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        FarmChange.verify = function (message, _depth) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                return "max depth exceeded";
            if (message.ownerId != null && $Object.hasOwnProperty.call(message, "ownerId"))
                if (!$util.isInteger(message.ownerId) && !(message.ownerId && $util.isInteger(message.ownerId.low) && $util.isInteger(message.ownerId.high)))
                    return "ownerId: integer|Long expected";
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion"))
                if (!$util.isInteger(message.farmVersion) && !(message.farmVersion && $util.isInteger(message.farmVersion.low) && $util.isInteger(message.farmVersion.high)))
                    return "farmVersion: integer|Long expected";
            if (message.changedPlots != null && $Object.hasOwnProperty.call(message, "changedPlots")) {
                if (!$Array.isArray(message.changedPlots))
                    return "changedPlots: array expected";
                for (var i = 0; i < message.changedPlots.length; ++i) {
                    var error = $root.farm.PlotView.verify(message.changedPlots[i], _depth + 1);
                    if (error)
                        return "changedPlots." + error;
                }
            }
            return null;
        };

        /**
         * Creates a FarmChange message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof farm.FarmChange
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {farm.FarmChange} FarmChange
         */
        FarmChange.fromObject = function (object, _depth) {
            if (object instanceof $root.farm.FarmChange)
                return object;
            if (!$util.isObject(object))
                throw $TypeError(".farm.FarmChange: object expected");
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var message = new $root.farm.FarmChange();
            if (object.ownerId != null)
                if (typeof object.ownerId === "object" ? object.ownerId.low || object.ownerId.high : $Number(object.ownerId) !== 0)
                    if ($util.Long)
                        message.ownerId = $util.Long.fromValue(object.ownerId, true);
                    else if (typeof object.ownerId === "string")
                        message.ownerId = $parseInt(object.ownerId, 10);
                    else if (typeof object.ownerId === "number")
                        message.ownerId = object.ownerId;
                    else if (typeof object.ownerId === "object")
                        message.ownerId = new $util.LongBits(object.ownerId.low >>> 0, object.ownerId.high >>> 0).toNumber(true);
            if (object.farmVersion != null)
                if (typeof object.farmVersion === "object" ? object.farmVersion.low || object.farmVersion.high : $Number(object.farmVersion) !== 0)
                    if ($util.Long)
                        message.farmVersion = $util.Long.fromValue(object.farmVersion, true);
                    else if (typeof object.farmVersion === "string")
                        message.farmVersion = $parseInt(object.farmVersion, 10);
                    else if (typeof object.farmVersion === "number")
                        message.farmVersion = object.farmVersion;
                    else if (typeof object.farmVersion === "object")
                        message.farmVersion = new $util.LongBits(object.farmVersion.low >>> 0, object.farmVersion.high >>> 0).toNumber(true);
            if (object.changedPlots) {
                if (!$Array.isArray(object.changedPlots))
                    throw $TypeError(".farm.FarmChange.changedPlots: array expected");
                message.changedPlots = $Array(object.changedPlots.length);
                for (var i = 0; i < object.changedPlots.length; ++i) {
                    if (!$util.isObject(object.changedPlots[i]))
                        throw $TypeError(".farm.FarmChange.changedPlots: object expected");
                    message.changedPlots[i] = $root.farm.PlotView.fromObject(object.changedPlots[i], _depth + 1);
                }
            }
            return message;
        };

        /**
         * Creates a plain object from a FarmChange message. Also converts values to other types if specified.
         * @function toObject
         * @memberof farm.FarmChange
         * @static
         * @param {farm.FarmChange} message FarmChange
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        FarmChange.toObject = function (message, options, _depth) {
            if (!options)
                options = {};
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var object = {};
            if (options.arrays || options.defaults)
                object.changedPlots = [];
            if (options.defaults) {
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.ownerId = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.ownerId = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.farmVersion = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.farmVersion = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
            }
            if (message.ownerId != null && $Object.hasOwnProperty.call(message, "ownerId"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.ownerId = typeof message.ownerId === "number" ? $BigInt(message.ownerId) : $util.Long.fromBits(message.ownerId.low >>> 0, message.ownerId.high >>> 0, true).toBigInt();
                else if (typeof message.ownerId === "number")
                    object.ownerId = options.longs === $String ? $String(message.ownerId) : message.ownerId;
                else
                    object.ownerId = options.longs === $String ? $util.Long.prototype.toString.call(message.ownerId) : options.longs === $Number ? new $util.LongBits(message.ownerId.low >>> 0, message.ownerId.high >>> 0).toNumber(true) : message.ownerId;
            if (message.farmVersion != null && $Object.hasOwnProperty.call(message, "farmVersion"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.farmVersion = typeof message.farmVersion === "number" ? $BigInt(message.farmVersion) : $util.Long.fromBits(message.farmVersion.low >>> 0, message.farmVersion.high >>> 0, true).toBigInt();
                else if (typeof message.farmVersion === "number")
                    object.farmVersion = options.longs === $String ? $String(message.farmVersion) : message.farmVersion;
                else
                    object.farmVersion = options.longs === $String ? $util.Long.prototype.toString.call(message.farmVersion) : options.longs === $Number ? new $util.LongBits(message.farmVersion.low >>> 0, message.farmVersion.high >>> 0).toNumber(true) : message.farmVersion;
            if (message.changedPlots && message.changedPlots.length) {
                object.changedPlots = $Array(message.changedPlots.length);
                for (var j = 0; j < message.changedPlots.length; ++j)
                    object.changedPlots[j] = $root.farm.PlotView.toObject(message.changedPlots[j], options, _depth + 1);
            }
            return object;
        };

        /**
         * Converts this FarmChange to JSON.
         * @function toJSON
         * @memberof farm.FarmChange
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        FarmChange.prototype.toJSON = function() {
            return FarmChange.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the type url for FarmChange
         * @function getTypeUrl
         * @memberof farm.FarmChange
         * @static
         * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
         * @returns {string} The type url
         */
        FarmChange.getTypeUrl = function(prefix) {
            if (prefix === $undefined)
                prefix = "type.googleapis.com";
            return prefix + "/farm.FarmChange";
        };

        return FarmChange;
    })();

    farm.ResyncRequired = (function() {

        /**
         * Properties of a ResyncRequired.
         * @typedef {Object} farm.ResyncRequired.$Properties
         * @property {string|null} [reason] ResyncRequired reason
         * @property {number|Long|null} [latestKnownFarmVersion] ResyncRequired latestKnownFarmVersion
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */

        /**
         * Properties of a ResyncRequired.
         * @memberof farm
         * @interface IResyncRequired
         * @augments farm.ResyncRequired.$Properties
         * @deprecated Use farm.ResyncRequired.$Properties instead.
         */

        /**
         * Shape of a ResyncRequired.
         * @typedef {farm.ResyncRequired.$Properties} farm.ResyncRequired.$Shape
         */

        /**
         * Constructs a new ResyncRequired.
         * @memberof farm
         * @classdesc Represents a ResyncRequired.
         * @constructor
         * @param {farm.ResyncRequired.$Properties=} [properties] Properties to set
         * @property {Array.<Uint8Array>} [$unknowns] Unknown fields preserved while decoding when enabled
         */
        var ResyncRequired = function (properties) {
            if (properties)
                for (var keys = $Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null && keys[i] !== "__proto__")
                        this[keys[i]] = properties[keys[i]];
        };

        /**
         * ResyncRequired reason.
         * @member {string} reason
         * @memberof farm.ResyncRequired
         * @instance
         */
        ResyncRequired.prototype.reason = "";

        /**
         * ResyncRequired latestKnownFarmVersion.
         * @member {number|Long} latestKnownFarmVersion
         * @memberof farm.ResyncRequired
         * @instance
         */
        ResyncRequired.prototype.latestKnownFarmVersion = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

        /**
         * Creates a new ResyncRequired instance using the specified properties.
         * @function create
         * @memberof farm.ResyncRequired
         * @static
         * @param {farm.ResyncRequired.$Properties=} [properties] Properties to set
         * @returns {farm.ResyncRequired} ResyncRequired instance
         * @type {{
         *   (properties: farm.ResyncRequired.$Shape): farm.ResyncRequired & farm.ResyncRequired.$Shape;
         *   (properties?: farm.ResyncRequired.$Properties): farm.ResyncRequired;
         * }}
         */
        ResyncRequired.create = function(properties) {
            return new ResyncRequired(properties);
        };

        /**
         * Encodes the specified ResyncRequired message. Does not implicitly {@link farm.ResyncRequired.verify|verify} messages.
         * @function encode
         * @memberof farm.ResyncRequired
         * @static
         * @param {farm.ResyncRequired.$Properties} message ResyncRequired message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ResyncRequired.encode = function (message, writer, _depth) {
            if (!writer)
                writer = $Writer.create();
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            if (message.reason != null && $Object.hasOwnProperty.call(message, "reason") && message.reason !== "")
                writer.uint32(/* id 1, wireType 2 =*/10).string(message.reason);
            if (message.latestKnownFarmVersion != null && $Object.hasOwnProperty.call(message, "latestKnownFarmVersion") && (typeof message.latestKnownFarmVersion === "object" ? message.latestKnownFarmVersion.low || message.latestKnownFarmVersion.high : message.latestKnownFarmVersion !== 0))
                writer.uint32(/* id 2, wireType 0 =*/16).uint64(message.latestKnownFarmVersion);
            if (message.$unknowns != null && $Object.hasOwnProperty.call(message, "$unknowns"))
                for (var i = 0; i < message.$unknowns.length; ++i)
                    writer.raw(message.$unknowns[i]);
            return writer;
        };

        /**
         * Encodes the specified ResyncRequired message, length delimited. Does not implicitly {@link farm.ResyncRequired.verify|verify} messages.
         * @function encodeDelimited
         * @memberof farm.ResyncRequired
         * @static
         * @param {farm.ResyncRequired.$Properties} message ResyncRequired message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ResyncRequired.encodeDelimited = function(message, writer) {
            return this.encode(message, (writer || $Writer.create()).fork()).ldelim();
        };

        /**
         * Decodes a ResyncRequired message from the specified reader or buffer.
         * @function decode
         * @memberof farm.ResyncRequired
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {farm.ResyncRequired & farm.ResyncRequired.$Shape} ResyncRequired
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ResyncRequired.decode = function (reader, length, _end, _depth, _target) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $Reader.recursionLimit)
                throw $Error("max depth exceeded");
            var end = length === $undefined ? reader.len : reader.pos + length, message = _target || new $root.farm.ResyncRequired(), value;
            while (reader.pos < end) {
                var start = reader.pos;
                var tag = reader.tag();
                if (tag === _end) {
                    _end = $undefined;
                    break;
                }
                var wireType = tag & 7;
                switch (tag >>>= 3) {
                case 1: {
                        if (wireType !== 2)
                            break;
                        if ((value = reader.stringVerify()).length)
                            message.reason = value;
                        else
                            delete message.reason;
                        continue;
                    }
                case 2: {
                        if (wireType !== 0)
                            break;
                        if (typeof (value = reader.uint64()) === "object" ? value.low || value.high : value !== 0)
                            message.latestKnownFarmVersion = value;
                        else
                            delete message.latestKnownFarmVersion;
                        continue;
                    }
                }
                reader.skipType(wireType, _depth, tag);
                if (!reader.discardUnknown) {
                    $util.makeProp(message, "$unknowns", false);
                    (message.$unknowns || (message.$unknowns = [])).push(reader.raw(start, reader.pos));
                }
            }
            if (_end !== $undefined)
                throw $Error("missing end group");
            return message;
        };

        /**
         * Decodes a ResyncRequired message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof farm.ResyncRequired
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {farm.ResyncRequired & farm.ResyncRequired.$Shape} ResyncRequired
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ResyncRequired.decodeDelimited = function(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a ResyncRequired message.
         * @function verify
         * @memberof farm.ResyncRequired
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        ResyncRequired.verify = function (message, _depth) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                return "max depth exceeded";
            if (message.reason != null && $Object.hasOwnProperty.call(message, "reason"))
                if (!$util.isString(message.reason))
                    return "reason: string expected";
            if (message.latestKnownFarmVersion != null && $Object.hasOwnProperty.call(message, "latestKnownFarmVersion"))
                if (!$util.isInteger(message.latestKnownFarmVersion) && !(message.latestKnownFarmVersion && $util.isInteger(message.latestKnownFarmVersion.low) && $util.isInteger(message.latestKnownFarmVersion.high)))
                    return "latestKnownFarmVersion: integer|Long expected";
            return null;
        };

        /**
         * Creates a ResyncRequired message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof farm.ResyncRequired
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {farm.ResyncRequired} ResyncRequired
         */
        ResyncRequired.fromObject = function (object, _depth) {
            if (object instanceof $root.farm.ResyncRequired)
                return object;
            if (!$util.isObject(object))
                throw $TypeError(".farm.ResyncRequired: object expected");
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var message = new $root.farm.ResyncRequired();
            if (object.reason != null)
                if (typeof object.reason !== "string" || object.reason.length)
                    message.reason = $String(object.reason);
            if (object.latestKnownFarmVersion != null)
                if (typeof object.latestKnownFarmVersion === "object" ? object.latestKnownFarmVersion.low || object.latestKnownFarmVersion.high : $Number(object.latestKnownFarmVersion) !== 0)
                    if ($util.Long)
                        message.latestKnownFarmVersion = $util.Long.fromValue(object.latestKnownFarmVersion, true);
                    else if (typeof object.latestKnownFarmVersion === "string")
                        message.latestKnownFarmVersion = $parseInt(object.latestKnownFarmVersion, 10);
                    else if (typeof object.latestKnownFarmVersion === "number")
                        message.latestKnownFarmVersion = object.latestKnownFarmVersion;
                    else if (typeof object.latestKnownFarmVersion === "object")
                        message.latestKnownFarmVersion = new $util.LongBits(object.latestKnownFarmVersion.low >>> 0, object.latestKnownFarmVersion.high >>> 0).toNumber(true);
            return message;
        };

        /**
         * Creates a plain object from a ResyncRequired message. Also converts values to other types if specified.
         * @function toObject
         * @memberof farm.ResyncRequired
         * @static
         * @param {farm.ResyncRequired} message ResyncRequired
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        ResyncRequired.toObject = function (message, options, _depth) {
            if (!options)
                options = {};
            if (_depth === $undefined)
                _depth = 0;
            if (_depth > $util.recursionLimit)
                throw $Error("max depth exceeded");
            var object = {};
            if (options.defaults) {
                object.reason = "";
                if ($util.Long) {
                    var long = new $util.Long(0, 0, true);
                    object.latestKnownFarmVersion = options.longs === $String ? long.toString() : options.longs === $Number ? long.toNumber() : typeof $BigInt !== "undefined" && options.longs === $BigInt ? long.toBigInt() : long;
                } else
                    object.latestKnownFarmVersion = options.longs === $String ? "0" : typeof $BigInt !== "undefined" && options.longs === $BigInt ? $BigInt("0") : 0;
            }
            if (message.reason != null && $Object.hasOwnProperty.call(message, "reason"))
                object.reason = message.reason;
            if (message.latestKnownFarmVersion != null && $Object.hasOwnProperty.call(message, "latestKnownFarmVersion"))
                if (typeof $BigInt !== "undefined" && options.longs === $BigInt)
                    object.latestKnownFarmVersion = typeof message.latestKnownFarmVersion === "number" ? $BigInt(message.latestKnownFarmVersion) : $util.Long.fromBits(message.latestKnownFarmVersion.low >>> 0, message.latestKnownFarmVersion.high >>> 0, true).toBigInt();
                else if (typeof message.latestKnownFarmVersion === "number")
                    object.latestKnownFarmVersion = options.longs === $String ? $String(message.latestKnownFarmVersion) : message.latestKnownFarmVersion;
                else
                    object.latestKnownFarmVersion = options.longs === $String ? $util.Long.prototype.toString.call(message.latestKnownFarmVersion) : options.longs === $Number ? new $util.LongBits(message.latestKnownFarmVersion.low >>> 0, message.latestKnownFarmVersion.high >>> 0).toNumber(true) : message.latestKnownFarmVersion;
            return object;
        };

        /**
         * Converts this ResyncRequired to JSON.
         * @function toJSON
         * @memberof farm.ResyncRequired
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        ResyncRequired.prototype.toJSON = function() {
            return ResyncRequired.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the type url for ResyncRequired
         * @function getTypeUrl
         * @memberof farm.ResyncRequired
         * @static
         * @param {string} [prefix] Custom type url prefix, defaults to `"type.googleapis.com"`
         * @returns {string} The type url
         */
        ResyncRequired.getTypeUrl = function(prefix) {
            if (prefix === $undefined)
                prefix = "type.googleapis.com";
            return prefix + "/farm.ResyncRequired";
        };

        return ResyncRequired;
    })();

    return farm;
})();

module.exports = $root;
