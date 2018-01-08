import * as _ from 'lodash';

type ArgsFunction = (...args: any[]) => void;

export const promiseFn = (fn: ArgsFunction, receiver) => {
  return (...args) => {
    return new Promise((resolve, reject) => {
      fn.apply(receiver, [...args, (err, res) => {
        return err ? reject(err) : resolve(res);
      }]);
    });
  };
};

export const generateStringArray = (items: string | string[]) => {
  if (!items) {
    return [];
  }
  if (_.isString(items) ) {
    return [items];
  }
  return items.filter((item) => item);
};

export const generateTrimStringArray = (items: string | string[]) => {
  return generateStringArray(items).filter(item => item.trim());
};
