/*
 * Copyright 2025 The Kubernetes Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { KubeDeployment } from '@kinvolk/headlamp-plugin/lib/k8s/deployment';
import { useQuery } from '@tanstack/react-query';
import { useSnackbar } from 'notistack';
import React from 'react';
import { useNotify } from '../../../common/notifications/useNotify';
import {
  initialRestartState,
  MAX_ROLLOUT_TIMEOUT_MS,
  REQUEST_TIMEOUT_MS,
  type RestartableDeployment,
  restartReducer,
} from './restartMachine';
import { RESTARTED_AT_ANNOTATION } from './rollout';

type UseRestartRolloutOptions = {
  /** Identifies the Knative Service, for the cache key of the fresh read. */
  serviceKey: string;
  /** The Service's Deployments as the watch last delivered them; null until the first list. */
  deployments: readonly KubeDeployment[] | null;
  /** The Service's Deployments from one read made when the restart began; null until it answers. */
  start: readonly RestartableDeployment[] | null;
  /** Why the Deployments could not be listed, if they could not. */
  listError: unknown;
  /** Reads the Service's Deployments once, without the watch. */
  readDeployments: () => Promise<readonly KubeDeployment[]>;
  /**
   * Called once the result has been shown.
   *
   * @param callOnDone - Whether the restart reached the running Deployments, so that the caller's
   * `onDone` is due.
   */
  onFinished: (callOnDone: boolean) => void;
};

/**
 * Waits for a request, but no longer than {@link REQUEST_TIMEOUT_MS}.
 *
 * @param request - The request to wait for.
 * @param what - What the request does, for the timeout error: `Timed out ${what}`.
 * @returns The request's result.
 * @throws The request's own error, or a timeout error.
 */
function withTimeout<T>(request: Promise<T>, what: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out ${what}`)), REQUEST_TIMEOUT_MS);
    request.then(
      value => {
        clearTimeout(timer);
        resolve(value);
      },
      err => {
        clearTimeout(timer);
        reject(err);
      }
    );
  });
}

/**
 * Restarts a Knative Service's running Deployments the way `kubectl rollout restart` does and
 * follows their rollouts until the result can be shown. It sets the
 * `kubectl.kubernetes.io/restartedAt` annotation on the Pod template of each Deployment that has
 * running Pods, follows every rollout through the Deployments the caller keeps up to date, and
 * shows the result. A patch that gets no answer in time, or fails without the API server refusing
 * it, is still followed, since the API server may have applied it; its rollout counts only once the
 * Deployment shows this restart's annotation. A Deployment that the watch lacks counts as removed
 * only once a fresh read lacks it too. When the rollouts make no progress for as long as the
 * Deployments allow, one fresh read settles what is left.
 *
 * The restart begins as soon as the read it starts from has arrived, so the component that uses
 * this hook is meant to live only as long as the restart.
 *
 * @param options - The Deployments to restart and how to read them again.
 */
export function useRestartRollout({
  serviceKey,
  deployments,
  start,
  listError,
  readDeployments,
  onFinished,
}: UseRestartRolloutOptions) {
  const [{ phase, outbox }, dispatch] = React.useReducer(restartReducer, initialRestartState);
  const { notifyError, notifyInfo } = useNotify();
  const { enqueueSnackbar, closeSnackbar } = useSnackbar();

  const onFinishedRef = React.useRef(onFinished);
  React.useEffect(() => {
    onFinishedRef.current = onFinished;
  });

  const listing = phase.step === 'listing';
  const patching = phase.step === 'patching' ? phase : null;
  const rolling = phase.step === 'rolling';
  const confirming = phase.step === 'confirming';
  const restartedAt =
    phase.step === 'rolling' || phase.step === 'confirming' ? phase.restartedAt : '';
  const idleMs = phase.step === 'rolling' ? phase.idleMs : 0;
  const watchedPending =
    phase.step === 'rolling'
      ? Array.from(phase.pending.keys(), name =>
          deployments?.find(item => item.metadata.name === name)
        )
      : [];
  const activityKey = watchedPending.map(item => item?.metadata.resourceVersion).join(',');
  // Set while the watch lacks a followed Deployment; it changes with every watch update then.
  const missingKey =
    deployments && watchedPending.includes(undefined)
      ? deployments.map(item => `${item.metadata.name}@${item.metadata.resourceVersion}`).join(',')
      : null;
  const progress = phase.step === 'listing' || phase.step === 'done' ? null : phase.progress;

  // Start from the read made when the restart began, or give up when there is none.
  React.useEffect(() => {
    if (!listing) {
      return;
    }
    if (listError) {
      dispatch({ type: 'failed', error: listError });
    } else if (start) {
      dispatch({ type: 'listed', deployments: start, restartedAt: new Date().toISOString() });
    }
  }, [listing, start, listError]);

  React.useEffect(() => {
    if (!listing) {
      return;
    }
    const timer = setTimeout(
      () => dispatch({ type: 'failed', error: new Error('Timed out listing Deployments') }),
      REQUEST_TIMEOUT_MS
    );
    return () => clearTimeout(timer);
  }, [listing]);

  // Patch the running Deployments. The patch is the same when it is sent twice.
  React.useEffect(() => {
    if (!patching) {
      return;
    }
    let cancelled = false;
    const body = {
      spec: {
        template: {
          metadata: { annotations: { [RESTARTED_AT_ANNOTATION]: patching.restartedAt } },
        },
      },
    };
    Promise.allSettled(
      patching.targets.map(({ deployment }) =>
        withTimeout<KubeDeployment | undefined>(deployment.patch(body), 'patching Deployment')
      )
    ).then(outcomes => {
      if (!cancelled) {
        dispatch({ type: 'patched', outcomes });
      }
    });
    return () => {
      cancelled = true;
    };
  }, [patching]);

  // Give up when the followed rollouts stay quiet for `idleMs`; any change to them starts the wait
  // over, because `activityKey` changes with them.
  React.useEffect(() => {
    if (!rolling) {
      return;
    }
    const timer = setTimeout(() => dispatch({ type: 'gaveUp' }), idleMs);
    return () => clearTimeout(timer);
  }, [rolling, idleMs, activityKey]);

  // Give up after an hour at the latest, however much the rollouts keep changing.
  React.useEffect(() => {
    if (!rolling) {
      return;
    }
    const timer = setTimeout(() => dispatch({ type: 'gaveUp' }), MAX_ROLLOUT_TIMEOUT_MS);
    return () => clearTimeout(timer);
  }, [rolling]);

  // One fresh read: it confirms a Deployment the watch lacks while rolling, and settles what is left
  // once the wait has given up.
  const confirmation = useQuery({
    enabled: missingKey !== null || confirming,
    queryKey: ['knative', 'restart-confirmation', serviceKey, restartedAt, missingKey],
    queryFn: () => withTimeout(readDeployments(), 'listing Deployments'),
    retry: false,
    gcTime: 0,
  });

  // Follow the rollouts. A cached watch list may omit a new Deployment; only a fresh read can
  // confirm its removal.
  const observed = missingKey === null ? deployments : confirmation.data;
  React.useEffect(() => {
    if (rolling && observed) {
      dispatch({ type: 'observed', deployments: observed });
    }
  }, [rolling, observed]);

  // Settle what is left with the fresh read.
  React.useEffect(() => {
    if (!confirming) {
      return;
    }
    if (confirmation.isSuccess) {
      dispatch({ type: 'confirmed', deployments: confirmation.data });
    } else if (confirmation.isError) {
      dispatch({ type: 'confirmFailed', error: confirmation.error });
    }
  }, [
    confirming,
    confirmation.isSuccess,
    confirmation.isError,
    confirmation.data,
    confirmation.error,
  ]);

  // Show the messages, and tell the caller once they have been shown.
  React.useEffect(() => {
    if (outbox.length === 0) {
      return;
    }
    outbox.forEach(notice => (notice.level === 'error' ? notifyError : notifyInfo)(notice.message));
    dispatch({ type: 'delivered' });
  }, [outbox, notifyError, notifyInfo]);

  React.useEffect(() => {
    if (phase.step === 'done' && outbox.length === 0) {
      onFinishedRef.current(phase.callOnDone);
    }
  }, [phase, outbox]);

  // Keep the progress message on screen for as long as the restart is under way.
  React.useEffect(() => {
    if (progress === null) {
      return;
    }
    const key = enqueueSnackbar(progress, { variant: 'info', persist: true });
    return () => closeSnackbar(key);
  }, [progress, enqueueSnackbar, closeSnackbar]);
}
