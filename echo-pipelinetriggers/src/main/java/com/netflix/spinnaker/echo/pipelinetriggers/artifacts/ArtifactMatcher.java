/*
 * Copyright 2017 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.echo.pipelinetriggers.artifacts;

import com.google.gson.Gson;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.netflix.spinnaker.echo.model.Trigger;
import com.netflix.spinnaker.kork.artifacts.model.Artifact;
import com.netflix.spinnaker.kork.artifacts.model.ExpectedArtifact;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class ArtifactMatcher {

  private static final Gson gson = new Gson();

  private static final Configuration configuration;

  static {
    configuration = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS).build();
  }

  public static boolean anyArtifactsMatchExpected(
      List<Artifact> messageArtifacts,
      Trigger trigger,
      List<ExpectedArtifact> pipelineExpectedArtifacts) {
    messageArtifacts = messageArtifacts == null ? new ArrayList<>() : messageArtifacts;
    List<String> expectedArtifactIds = trigger.getExpectedArtifactIds();

    if (expectedArtifactIds == null || expectedArtifactIds.isEmpty()) {
      return true;
    }

    List<ExpectedArtifact> expectedArtifacts =
        pipelineExpectedArtifacts == null
            ? new ArrayList<>()
            : pipelineExpectedArtifacts.stream()
                .filter(e -> expectedArtifactIds.contains(e.getId()))
                .collect(Collectors.toList());

    if (messageArtifacts.size() > expectedArtifactIds.size()) {
      log.warn(
          "Parsed message artifacts (size {}) greater than expected artifacts (size {}), continuing trigger anyway",
          messageArtifacts.size(),
          expectedArtifactIds.size());
    }

    Predicate<Artifact> expectedArtifactMatch =
        a -> expectedArtifacts.stream().anyMatch(e -> e.matches(a));

    boolean result = messageArtifacts.stream().anyMatch(expectedArtifactMatch);
    if (!result) {
      log.info("Skipping trigger {} as artifact constraints were not satisfied", trigger);
    }
    return result;
  }

  /**
   * Check that there is a key in the payload for each constraint declared in a Trigger. Also check
   * that if there is a value for a given key, that the value matches the value in the payload.
   * Constraint keys that begin with a '$.' are treated as JsonPath expressions which are used to
   * extract the value from the payload to test against.
   *
   * @param constraints A map of constraints configured in the Trigger (eg, created in Deck). A
   *     constraint is a [key, java regex value] pair.
   * @param payload A map of the payload contents POST'd in the triggering event.
   * @return Whether every key (and value if applicable) in the constraints map is represented in
   *     the payload.
   */
  public static boolean isConstraintInPayload(final Map constraints, final Map payload) {

    DocumentContext document =
        JsonPath.using(configuration).parse(gson.toJson(payload, LinkedHashMap.class));

    for (Object key : constraints.keySet()) {
      if (key.toString().startsWith("$.")
          && constraints.get(key) != null
          && document.read(key.toString()) != null
          && matches(constraints.get(key).toString(), document.read(key.toString()).toString())) {
        return true;
      }

      if (!payload.containsKey(key) || payload.get(key) == null) {
        return false;
      }

      if (constraints.get(key) != null
          && !matches(constraints.get(key).toString(), payload.get(key).toString())) {
        return false;
      }
    }
    return true;
  }

  private static boolean matches(String us, String other) {
    return Pattern.compile(us).asPredicate().test(other);
  }
}
