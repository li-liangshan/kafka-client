"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const _ = require("lodash");
exports.promiseFn = (fn, receiver) => {
    return (...args) => {
        return new Promise((resolve, reject) => {
            fn.apply(receiver, [...args, (err, res) => {
                    return err ? reject(err) : resolve(res);
                }]);
        });
    };
};
exports.generateStringArray = (items) => {
    if (!items) {
        return [];
    }
    if (_.isString(items)) {
        return [items];
    }
    return items.filter((item) => item);
};
exports.generateTrimStringArray = (items) => {
    return exports.generateStringArray(items).filter(item => item.trim());
};
//# sourceMappingURL=helper.js.map