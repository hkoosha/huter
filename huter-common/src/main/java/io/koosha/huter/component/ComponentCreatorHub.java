package io.koosha.huter.component;

import io.koosha.huter.runner.HuterContext;
import io.koosha.huter.util.PathToContentFun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public final class ComponentCreatorHub {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentCreatorHub.class);

    public static final String COMMAND_SEPARATOR_REGEX = "\\s";
    public static final String COMMENT_SEPARATOR_REGEX = "#";

    private final PathToContentFun reader;
    private final DatabaseCreator dbCreator;
    private final FileBasedTableCreator fileBasedTableCreator;
    private final FunctionCreator functionCreator;

    public ComponentCreatorHub(final PathToContentFun reader) {
        this.reader = reader;
        this.dbCreator = new DatabaseCreator();
        this.fileBasedTableCreator = new FileBasedTableCreator();
        this.functionCreator = new FunctionCreator();
    }

    public void createComponent(final HuterContext ctx,
                                final Path dataPath,
                                final String definition) throws Exception {
        if (definition.trim().startsWith(COMMENT_SEPARATOR_REGEX) || definition.trim().isEmpty())
            return;

        final String[] elements = definition.split(COMMAND_SEPARATOR_REGEX, 2);
        if (elements.length != 2)
            throw new IllegalArgumentException("bad component definition: " + definition);

        final String type = elements[0].trim();
        final String param = elements[1];

        LOGGER.debug("creating component type={}, param={}", type, param);

        switch (type.toUpperCase()) {
            case "DATABASE":
                this.dbCreator.create(ctx, this.reader, dataPath,
                    param.trim().split(COMMENT_SEPARATOR_REGEX)[0].trim());
                break;

            case "FUNCTION":
                this.functionCreator.create(ctx, this.reader, dataPath,
                    param.trim().split(COMMENT_SEPARATOR_REGEX)[0].trim());
                break;

            case "TABLEFILE":
            case "TABLE_FILE":
            case "TABLE":
            case "FILE":
                this.fileBasedTableCreator.create(ctx, this.reader, dataPath,
                    param.trim().split(COMMENT_SEPARATOR_REGEX)[0].trim());
                break;

            default:
                this.findComponentCreator(type).create(ctx, this.reader, dataPath, param);
        }
    }

    private ComponentCreator findComponentCreator(final String type) {
        final Class<?> handlerClass;
        try {
            handlerClass = Class.forName(type);
        }
        catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("no component creator registered for the given type=" + type);
        }

        try {
            return (ComponentCreator) handlerClass.newInstance();
        }
        catch (final ClassCastException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("failed to instantiate handler for type=" + type, e);
        }
    }

}
