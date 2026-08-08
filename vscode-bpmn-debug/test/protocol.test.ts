import { describe, expect, it } from 'vitest';

import { markerDelta } from '../src/protocol.js';

describe('markerDelta', () => {
  it('adds newly-active elements and removes no-longer-active ones', () => {
    expect(markerDelta(['a', 'b'], ['b', 'c'])).toEqual({ add: ['c'], remove: ['a'] });
  });

  it('is a no-op when the highlight set is unchanged', () => {
    expect(markerDelta(['a', 'b'], ['a', 'b'])).toEqual({ add: [], remove: [] });
  });

  it('adds all on first highlight', () => {
    expect(markerDelta([], ['s'])).toEqual({ add: ['s'], remove: [] });
  });

  it('removes all on clear (termination)', () => {
    expect(markerDelta(['first', 'second'], [])).toEqual({
      add: [],
      remove: ['first', 'second'],
    });
  });
});
