export interface Action {
  type: string;
  payload: any;
}

export type Reducer<State> = (arg0: State, arg1: Action) => State;

export type EffectHandler<State> = (arg0: Action, arg1: State) => void;

export class Store<State> {
  state: State;
  reducers: [RegExp, Reducer<State>][];
  effectHandlers: [RegExp, EffectHandler<State>][];

  constructor(state: State) {
    this.state = state;
    this.reducers = [];
    this.effectHandlers = [];
  }

  dispatch(action: Action) {
    // console.log("BEFORE STATE:", this.state);

    // console.log("ACTION:", action);

    for (let [pat, red] of this.reducers) {
      if (pat.test(action.type)) {
        this.state = red(this.state, action);
      }
    }

    // console.log("AFTER STATE:", this.state);

    for (let [pat, eff] of this.effectHandlers) {
      if (pat.test(action.type)) {
        eff(action, this.state);
      }
    }
  }

  on(
    pattern: RegExp | string,
    reducer: Reducer<State> | null = null,
    effectHandler: EffectHandler<State> | null = null
  ) {
    if (typeof pattern === "string") {
      const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      pattern = new RegExp(`^${escaped}$`);
    }
    if (reducer) {
      this.reducers.push([pattern, reducer]);
    }
    if (effectHandler) {
      this.effectHandlers.push([pattern, effectHandler]);
    }
  }
}
