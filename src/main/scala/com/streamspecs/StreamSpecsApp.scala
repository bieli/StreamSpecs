package com.streamspecs

import cats.effect.{ExitCode, IO, IOApp}

object StreamSpecsApp extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    IO.println("\n\n===========\nStreamSpecs.") *>
    IO.pure(ExitCode.Success)
  end run

end StreamSpecsApp
