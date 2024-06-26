import { createLogger, format, transports } from "winston";

const logger = createLogger({
  level: "debug",
  format: format.combine(
    format.colorize({ all: true }),
    format.timestamp({ format: "HH:mm:ss" }),
    format.printf((info) => `[${info.timestamp}] ${info.level}: ${info.message}`)
  ),
  transports: [new transports.Console()],
});

export default logger;
